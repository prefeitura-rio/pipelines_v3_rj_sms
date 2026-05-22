import json
import os
import signal
import subprocess
from time import sleep

import pandas as pd
from google.cloud import bigquery
from prefect.context import FlowRunContext
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from pipelines.datalake.extract_load.vitacare_historico.constants import (
  vitacare_constants,
)
from pipelines.datalake.migrate.gcs_to_cloudsql.tasks import (
  start_instance as start_instance_task,
)
from pipelines.datalake.migrate.gcs_to_cloudsql.tasks import (
  stop_instance as stop_instance_task,
)
from pipelines.utils.cleanup import cleanup_columns_for_bigquery
from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.datetime import now, parse_date_or_today
from pipelines.utils.env import environment_is_valid, get_prefect_url
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def start_cloudsql_instance() -> None:
  log(
    f"(start_cloudsql_instance) ligando instância "
    f"'{vitacare_constants.INSTANCE_NAME.value}'"
  )
  start_instance_task.fn(instance_name=vitacare_constants.INSTANCE_NAME.value)


@task
def stop_cloudsql_instance() -> None:
  log(
    f"(stop_cloudsql_instance) desligando instância "
    f"'{vitacare_constants.INSTANCE_NAME.value}'"
  )
  try:
    stop_instance_task.fn(instance_name=vitacare_constants.INSTANCE_NAME.value)
  except Exception as exc:  # pylint: disable=broad-except
    log(
      f"(stop_cloudsql_instance) erro ao desligar instância "
      f"'{vitacare_constants.INSTANCE_NAME.value}': {repr(exc)}",
      level="error",
    )


@task
def validate_environment(environment: str) -> str:
  environment_is_valid(environment=environment)
  return environment


@task
def get_cnes_from_bigquery() -> list:
  # Ref: https://docs.cloud.google.com/bigquery/docs/reference/libraries#client-libraries-install-python
  query = """
    select distinct id_cnes
    from `rj-sms.saude_dados_mestres.estabelecimento`
    where prontuario_versao = 'vitacare'
    and prontuario_episodio_tem_dado = 'sim'
    """

  cnes_column = "id_cnes"

  try:
    client = bigquery.Client()

    rows = client.query_and_wait(query)

    cnes_list = [str(row[cnes_column]) for row in rows]

    log(f"(get_cnes_from_bigquery) {len(cnes_list)} cnes encontrados no bigquery")
    return cnes_list

  except Exception as e:
    log(f"(get_cnes_from_bigquery) erro ao buscar cnes do bigquery: {e}", level="error")
    raise


@task
def get_database_tables(environment) -> list:
  tables = get_secret(
    secret_name=vitacare_constants.INFISICAL_TABLES.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  table_names = json.loads(tables)
  log(f"(get_database_tables) {len(table_names)} tabela(s) encontradas")
  return table_names


@task
def start_cloudsql_proxy(environment: str) -> int:
  connection_name = get_secret(
    secret_name=vitacare_constants.INFISICAL_CONNECTION_NAME.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  port = get_secret(
    secret_name=vitacare_constants.INFISICAL_PORT.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  # O proxy abre uma porta local e encaminha tudo para o Cloud SQL.
  # Ref: https://cloud.google.com/sql/docs/sqlserver/connect-auth-proxy
  command = [
    "cloud-sql-proxy",
    f"--address={vitacare_constants.LOCAL_DATABASE_HOST.value}",
    f"--port={port}",
    connection_name,
  ]
  log(
    "(start_cloudsql_proxy) iniciando Cloud SQL Auth Proxy em "
    f"{vitacare_constants.LOCAL_DATABASE_HOST.value}:{port}"
  )
  process = subprocess.Popen(command)

  # Tempo simples para o processo abrir a porta antes da próxima task conectar.
  sleep(5)
  return process.pid


@task
def stop_cloudsql_proxy(process_id: int):
  if not process_id:
    return

  log(f"(stop_cloudsql_proxy) encerrando Cloud SQL Auth Proxy: processo {process_id}")
  try:
    os.kill(process_id, signal.SIGTERM)
  except ProcessLookupError:
    return


@task
def get_database_engine(database_name: str, environment: str):
  username = get_secret(
    secret_name=vitacare_constants.INFISICAL_USERNAME.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  password = get_secret(
    secret_name=vitacare_constants.INFISICAL_PASSWORD.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  port = get_secret(
    secret_name=vitacare_constants.INFISICAL_PORT.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  log(f"(get_database_engine) criando engine para database '{database_name}'")

  # A engine conecta no proxy local. O proxy encaminha para a instância Cloud SQL.
  # Ref: https://cloud.google.com/sql/docs/sqlserver/samples/cloud-sql-sqlserver-sqlalchemy-connect-tcp
  database_url = URL.create(
    drivername="mssql+pytds",
    username=username,
    password=password,
    port=port,
    database=database_name,
    host=vitacare_constants.LOCAL_DATABASE_HOST.value,
  )
  return create_engine(database_url)


@task(task_run_name="{cnes} - {table_name}")
def extract_table_to_bigquery(
  database_name: str,
  environment: str,
  cnes: str,
  table_name: str,
  chunk_size: int = 500_000,
) -> dict:
  started_at = now()
  destination_table = table_name.lower()
  query = f"select * from [dbo].[{table_name}]"
  total_rows = 0

  result = {
    "cnes": cnes,
    "database_name": database_name,
    "source_table": table_name,
    "destination_dataset": vitacare_constants.DESTINATION_DATASET.value,
    "destination_table": destination_table,
    "status": "success",
    "error_message": None,
    "table_rows": 0,
    "started_at": started_at.isoformat(),
    "finished_at": None,
  }

  log(
    f"(extract_table_to_bigquery) extraindo tabela '{table_name}' do CNES '{cnes}' "
    f"para {vitacare_constants.DESTINATION_DATASET.value}.{destination_table}"
  )

  engine = None
  current_chunk = None
  try:
    engine = get_database_engine.fn(database_name=database_name, environment=environment)
    chunks = pd.read_sql_query(query, engine, chunksize=chunk_size)
    for chunk_index, dataframe in enumerate(chunks):
      current_chunk = chunk_index + 1
      if dataframe.empty:
        continue

      dataframe = cleanup_columns_for_bigquery(dataframe)
      dataframe["id_cnes"] = cnes
      dataframe["extracted_at"] = started_at
      dataframe_rows = len(dataframe)

      log(
        f"(extract_table_to_bigquery) enviando lote {current_chunk} "
        f"com {dataframe_rows} linha(s)"
      )
      upload_df_to_datalake(
        df=dataframe,
        dataset_id=vitacare_constants.DESTINATION_DATASET.value,
        table_id=destination_table,
        dump_mode="append",
        source_format="parquet",
        date_partition_column="extracted_at",
      )
      total_rows += dataframe_rows

    if total_rows == 0:
      result["status"] = "empty"
      result["error_message"] = f"Tabela '{table_name}' sem linhas para o CNES '{cnes}'."
      log(
        f"(extract_table_to_bigquery) tabela '{table_name}' do CNES '{cnes}' vazia",
        level="warning",
      )

  except Exception as exc:  # pylint: disable=broad-except
    error_text = str(exc)
    result["status"] = "failed"
    if "Invalid object name" in error_text:
      result["error_message"] = (
        f"Tabela '{table_name}' não encontrada na database '{database_name}'."
      )
    elif "Cannot open database" in error_text or "requested by the login" in error_text:
      result["error_message"] = (
        f"Database '{database_name}' não encontrada ou sem acesso."
      )
    elif "Login failed" in error_text:
      result["error_message"] = (
        f"Falha de autenticação ao conectar na database '{database_name}'."
      )
    elif current_chunk:
      result["error_message"] = (
        f"Extração incompleta: falha ao processar o lote {current_chunk} "
        f"após {total_rows} linha(s) enviada(s). Erro original: {error_text}"
      )
    else:
      result["error_message"] = (
        f"Falha inesperada antes de concluir a extração. Erro original: {error_text}"
      )
    log(f"(extract_table_to_bigquery) {result['error_message']}", level="error")

  finally:
    if engine:
      engine.dispose()
    result["table_rows"] = total_rows
    result["finished_at"] = now().isoformat()

  log(f"(extract_table_to_bigquery) extração finalizada: {result}")
  return result


@task(task_run_name="CNES {cnes}")
def extract_cnes_tables(
  environment: str, cnes: str, table_names: list[str]
) -> list[dict]:
  database_name = f"vitacare_historic_{cnes}"
  results = []

  log(
    f"(extract_cnes_tables) iniciando extração sequencial de "
    f"{len(table_names)} tabela(s) do CNES '{cnes}'"
  )

  for table_name in table_names:
    results.append(
      extract_table_to_bigquery(
        database_name=database_name,
        environment=environment,
        cnes=cnes,
        table_name=table_name,
      )
    )

  log(f"(extract_cnes_tables) extração do CNES '{cnes}' finalizada")
  return results


@task
def write_log(log_items: list[dict], log_table_id: str) -> dict:
  if not log_items:
    return {"inserted_rows": 0}

  flow_run_id = None
  flow_run_url = None
  flow_run_context = FlowRunContext.get()
  if flow_run_context:
    flow_run_id = str(flow_run_context.flow_run.id)
    flow_run_url = f"{get_prefect_url()}/runs/flow-run/{flow_run_id}"

  rows = []
  for log_item in log_items:
    timestamp = log_item["finished_at"] or now().isoformat()
    rows.append(
      {
        "flow_run_id": flow_run_id,
        "flow_run_url": flow_run_url,
        "cnes": log_item["cnes"],
        "database_name": log_item["database_name"],
        "source_table": log_item["source_table"],
        "destination_dataset": log_item["destination_dataset"],
        "destination_table": log_item["destination_table"],
        "status": log_item["status"],
        "error_message": log_item["error_message"],
        "table_rows": log_item["table_rows"],
        "started_at": log_item["started_at"],
        "finished_at": log_item["finished_at"],
        "timestamp": timestamp,
        "data_particao": parse_date_or_today(timestamp).date().isoformat(),
      }
    )

  client = bigquery.Client()
  errors = client.insert_rows_json(log_table_id, rows)
  if errors:
    raise RuntimeError(f"Erro inserindo logs no BigQuery: {errors}")

  log(f"(write_log) {len(rows)} linha(s) inserida(s) em '{log_table_id}'")
  return {"inserted_rows": len(rows)}
