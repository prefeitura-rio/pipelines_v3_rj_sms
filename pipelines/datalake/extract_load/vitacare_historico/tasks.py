import json
import os
import signal
import subprocess
from time import sleep

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from pipelines.datalake.extract_load.vitacare_historico.constants import (
  vitacare_constants,
)
from pipelines.utils.cleanup import cleanup_columns_for_bigquery
from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.datetime import now
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


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

    log(f"{len(cnes_list)} cnes encontrados no bigquery.")
    return cnes_list

  except Exception as e:
    log(f"Erro ao buscar cnes do bigquery: {e}", level="error")
    raise


@task
def get_database_tables(environment) -> list:
  tables = get_secret(
    secret_name=vitacare_constants.INFISICAL_TABLES.value,
    path=vitacare_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  return json.loads(tables)


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
    "Iniciando Cloud SQL Auth Proxy em "
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

  log(f"Encerrando Cloud SQL Auth Proxy: processo {process_id}")
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


@task
def extract_table_to_bigquery(
  engine, cnes: str, table_name: str, chunk_size: int = 500_000
) -> dict:
  destination_table = table_name.lower()
  query = f"select * from dbo.{table_name}"
  extracted_at = now()
  total_rows = 0

  log(
    f"Extraindo tabela '{table_name}' do CNES '{cnes}' "
    f"para {vitacare_constants.DESTINATION_DATASET.value}.{destination_table}"
  )

  chunks = pd.read_sql_query(query, engine, chunksize=chunk_size)
  for chunk_index, dataframe in enumerate(chunks):
    if dataframe.empty:
      continue

    dataframe = cleanup_columns_for_bigquery(dataframe)
    dataframe["id_cnes"] = cnes
    dataframe["extracted_at"] = extracted_at
    total_rows += len(dataframe)

    upload_df_to_datalake(
      df=dataframe,
      dataset_id=vitacare_constants.DESTINATION_DATASET.value,
      table_id=destination_table,
      dump_mode="replace" if chunk_index == 0 else "append",
      source_format="parquet",
      date_partition_column="extracted_at",
    )

  result = {
    "cnes": cnes,
    "source_table": table_name,
    "destination_table": destination_table,
    "rows": total_rows,
    "extracted_at": extracted_at,
  }
  log(f"Extração finalizada: {result}")
  return result
