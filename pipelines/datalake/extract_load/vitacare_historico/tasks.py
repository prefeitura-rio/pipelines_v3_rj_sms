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
from pipelines.utils.datetime import now_naive, parse_date_or_today
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


def _debug_fetch_cursor(cursor, label: str, fetch_size: int, max_rows: int | None):
  total_rows = 0
  batch_count = 0

  description = cursor.description or []
  columns = [column[0] for column in description]
  log(f"({label}) cursor aberto; {len(columns)} coluna(s): {columns}")

  while True:
    rows = cursor.fetchmany(fetch_size)
    if not rows:
      log(f"({label}) fetch finalizado sem novas linhas")
      break

    batch_count += 1
    total_rows += len(rows)
    log(
      f"({label}) lote {batch_count}: {len(rows)} linha(s); total acumulado={total_rows}"
    )

    if batch_count == 1 and rows:
      first_row = rows[0]
      sample = {}
      for index, column_name in enumerate(columns[:8]):
        value = first_row[index]
        value_text = repr(value)
        if len(value_text) > 300:
          value_text = value_text[:300] + "...<truncated>"
        sample[column_name] = {"type": type(value).__name__, "repr": value_text}
      log(f"({label}) amostra primeira linha: {sample}")

    if max_rows and total_rows >= max_rows:
      log(f"({label}) parada por max_rows={max_rows}")
      break

  return {"batches": batch_count, "rows": total_rows}


def _debug_pytds_connection(
  *,
  host: str,
  port: int,
  database_name: str,
  username: str,
  password: str,
  table_name: str,
  fetch_size: int,
  max_rows: int | None,
):
  import pytds  # pylint: disable=import-outside-toplevel

  log("(debug_vitacare_tds_pytds) import pytds OK")
  log(
    f"(debug_vitacare_tds_pytds) conectando em {host}:{port}, database='{database_name}'"
  )

  with pytds.connect(
    server=host,
    port=port,
    database=database_name,
    user=username,
    password=password,
    login_timeout=30,
    timeout=None,
    autocommit=True,
  ) as connection:
    log("(debug_vitacare_tds_pytds) conexão aberta")
    cursor = connection.cursor()
    try:
      log("(debug_vitacare_tds_pytds) executando select @@version")
      cursor.execute("select @@version")
      version_row = cursor.fetchone()
      log(f"(debug_vitacare_tds_pytds) @@version={version_row}")
    finally:
      cursor.close()

    cursor = connection.cursor()
    try:
      count_query = f"select count(*) from [dbo].[{table_name}]"
      log(f"(debug_vitacare_tds_pytds) executando count: {count_query}")
      cursor.execute(count_query)
      count_row = cursor.fetchone()
      log(f"(debug_vitacare_tds_pytds) count={count_row}")
    finally:
      cursor.close()

    cursor = connection.cursor()
    try:
      top_query = f"select top 1 * from [dbo].[{table_name}]"
      log(f"(debug_vitacare_tds_pytds) executando top 1: {top_query}")
      cursor.execute(top_query)
      top_result = _debug_fetch_cursor(
        cursor=cursor, label="debug_vitacare_tds_pytds_top_1", fetch_size=1, max_rows=1
      )
      log(f"(debug_vitacare_tds_pytds) top 1 finalizado: {top_result}")
    finally:
      cursor.close()

    cursor = connection.cursor()
    try:
      full_query = f"select * from [dbo].[{table_name}]"
      log(
        f"(debug_vitacare_tds_pytds) executando leitura completa: {full_query}; "
        f"fetch_size={fetch_size}; max_rows={max_rows}"
      )
      cursor.execute(full_query)
      full_result = _debug_fetch_cursor(
        cursor=cursor,
        label="debug_vitacare_tds_pytds_full",
        fetch_size=fetch_size,
        max_rows=max_rows,
      )
      log(f"(debug_vitacare_tds_pytds) leitura completa finalizada: {full_result}")
      return full_result
    finally:
      cursor.close()


def _debug_pyodbc_connection(
  *,
  host: str,
  port: int,
  database_name: str,
  username: str,
  password: str,
  table_name: str,
  fetch_size: int,
  max_rows: int | None,
):
  import pyodbc  # pylint: disable=import-outside-toplevel

  log("(debug_vitacare_tds_pyodbc) import pyodbc OK")
  log(f"(debug_vitacare_tds_pyodbc) drivers disponíveis: {pyodbc.drivers()}")

  connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={host},{port};"
    f"DATABASE={database_name};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
  )
  log(
    f"(debug_vitacare_tds_pyodbc) conectando em {host}:{port}, database='{database_name}'"
  )

  with pyodbc.connect(connection_string, autocommit=True) as connection:
    log("(debug_vitacare_tds_pyodbc) conexão aberta")
    cursor = connection.cursor()
    try:
      cursor.arraysize = fetch_size
      log("(debug_vitacare_tds_pyodbc) executando select @@version")
      cursor.execute("select @@version")
      version_row = cursor.fetchone()
      log(f"(debug_vitacare_tds_pyodbc) @@version={version_row}")
    finally:
      cursor.close()

    cursor = connection.cursor()
    try:
      cursor.arraysize = fetch_size
      count_query = f"select count(*) from [dbo].[{table_name}]"
      log(f"(debug_vitacare_tds_pyodbc) executando count: {count_query}")
      cursor.execute(count_query)
      count_row = cursor.fetchone()
      log(f"(debug_vitacare_tds_pyodbc) count={count_row}")
    finally:
      cursor.close()

    cursor = connection.cursor()
    try:
      cursor.arraysize = fetch_size
      full_query = f"select * from [dbo].[{table_name}]"
      log(
        f"(debug_vitacare_tds_pyodbc) executando leitura completa: {full_query}; "
        f"fetch_size={fetch_size}; max_rows={max_rows}"
      )
      cursor.execute(full_query)
      full_result = _debug_fetch_cursor(
        cursor=cursor,
        label="debug_vitacare_tds_pyodbc_full",
        fetch_size=fetch_size,
        max_rows=max_rows,
      )
      log(f"(debug_vitacare_tds_pyodbc) leitura completa finalizada: {full_result}")
      return full_result
    finally:
      cursor.close()


@task
def debug_vitacare_tds_connection(
  database_name: str,
  environment: str,
  table_name: str = "ATENDIMENTOS",
  fetch_size: int = 10_000,
  max_rows: int | None = None,
  run_pyodbc: bool = True,
) -> dict:
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
  port = int(
    get_secret(
      secret_name=vitacare_constants.INFISICAL_PORT.value,
      path=vitacare_constants.INFISICAL_PATH.value,
      environment=environment,
    )
  )
  host = vitacare_constants.LOCAL_DATABASE_HOST.value

  log(
    "(debug_vitacare_tds_connection) iniciando diagnóstico: "
    f"database='{database_name}', table='{table_name}', host='{host}', port={port}, "
    f"fetch_size={fetch_size}, max_rows={max_rows}, run_pyodbc={run_pyodbc}"
  )

  results = {}
  try:
    results["pytds"] = {
      "status": "success",
      "result": _debug_pytds_connection(
        host=host,
        port=port,
        database_name=database_name,
        username=username,
        password=password,
        table_name=table_name,
        fetch_size=fetch_size,
        max_rows=max_rows,
      ),
    }
  except Exception as exc:  # pylint: disable=broad-except
    results["pytds"] = {"status": "failed", "error": repr(exc)}
    log(f"(debug_vitacare_tds_connection) pytds falhou: {repr(exc)}", level="error")

  if run_pyodbc:
    try:
      results["pyodbc"] = {
        "status": "success",
        "result": _debug_pyodbc_connection(
          host=host,
          port=port,
          database_name=database_name,
          username=username,
          password=password,
          table_name=table_name,
          fetch_size=fetch_size,
          max_rows=max_rows,
        ),
      }
    except Exception as exc:  # pylint: disable=broad-except
      results["pyodbc"] = {"status": "failed", "error": repr(exc)}
      log(f"(debug_vitacare_tds_connection) pyodbc falhou: {repr(exc)}", level="error")

  log(f"(debug_vitacare_tds_connection) diagnóstico finalizado: {results}")
  return results


@task
def extract_table_to_bigquery(
  database_name: str,
  environment: str,
  cnes: str,
  table_name: str,
  chunk_size: int = 500_000,
) -> dict:
  started_at = now_naive()
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
    result["finished_at"] = now_naive().isoformat()

  log(f"(extract_table_to_bigquery) extração finalizada: {result}")
  return result


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
    timestamp = log_item["finished_at"] or now_naive().isoformat()
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
