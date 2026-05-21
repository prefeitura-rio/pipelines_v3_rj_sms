# -*- coding: utf-8 -*-
import os

from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import (
  DEFAULT_SAMPLE_QUERY,
  DEFAULT_SQLSERVER_DRIVER,
  DEFAULT_SQLSERVER_PORT,
)
from .utils import (
  build_sample_parquet_path,
  build_sqlserver_odbc_connection_string,
  hide_password,
)


def get_sqlserver_connection_string(
  host_secret_name: str,
  username_secret_name: str,
  password_secret_name: str,
  secret_path: str,
  database_name: str,
  port_secret_name: str = None,
  environment: str = "dev",
  port: int = DEFAULT_SQLSERVER_PORT,
  driver: str = DEFAULT_SQLSERVER_DRIVER,
) -> str:
  log(
    f"(get_sqlserver_connection_string) buscando secrets em '{secret_path}' "
    f"para database '{database_name}'"
  )
  host = get_secret(
    secret_name=host_secret_name, path=secret_path, environment=environment
  )
  username = get_secret(
    secret_name=username_secret_name, path=secret_path, environment=environment
  )
  password = get_secret(
    secret_name=password_secret_name, path=secret_path, environment=environment
  )
  if port_secret_name:
    log(
      f"(get_sqlserver_connection_string) buscando porta pelo secret '{port_secret_name}'"
    )
    port = int(
      get_secret(secret_name=port_secret_name, path=secret_path, environment=environment)
    )

  connection_string = build_sqlserver_odbc_connection_string(
    host=host,
    username=username,
    password=password,
    database_name=database_name,
    port=port,
    driver=driver,
  )
  log(
    "(get_sqlserver_connection_string) connection string: "
    f"{hide_password(connection_string)}"
  )
  return connection_string


@task
def test_sqlserver_connection(
  host_secret_name: str,
  username_secret_name: str,
  password_secret_name: str,
  secret_path: str,
  database_name: str,
  port_secret_name: str = None,
  environment: str = "dev",
  port: int = DEFAULT_SQLSERVER_PORT,
  driver: str = DEFAULT_SQLSERVER_DRIVER,
) -> dict:
  import pyodbc

  log(f"(test_sqlserver_connection) abrindo conexão com '{database_name}'")
  connection_string = get_sqlserver_connection_string(
    host_secret_name=host_secret_name,
    username_secret_name=username_secret_name,
    password_secret_name=password_secret_name,
    secret_path=secret_path,
    database_name=database_name,
    port_secret_name=port_secret_name,
    environment=environment,
    port=port,
    driver=driver,
  )

  with pyodbc.connect(connection_string, timeout=30) as connection:
    cursor = connection.cursor()
    row = cursor.execute("SELECT 1 AS connection_ok").fetchone()

  result = {"connection_ok": bool(row and row[0] == 1)}
  log(f"(test_sqlserver_connection) resultado: {result}")
  return result


@task
def extract_query_sample_to_parquet(
  host_secret_name: str,
  username_secret_name: str,
  password_secret_name: str,
  secret_path: str,
  database_name: str,
  port_secret_name: str = None,
  query: str = DEFAULT_SAMPLE_QUERY,
  environment: str = "dev",
  port: int = DEFAULT_SQLSERVER_PORT,
  driver: str = DEFAULT_SQLSERVER_DRIVER,
  output_dir: str = "/tmp",
) -> dict:
  import duckdb

  log(
    f"(extract_query_sample_to_parquet) preparando extração da database '{database_name}'"
  )
  connection_string = get_sqlserver_connection_string(
    host_secret_name=host_secret_name,
    username_secret_name=username_secret_name,
    password_secret_name=password_secret_name,
    secret_path=secret_path,
    database_name=database_name,
    port_secret_name=port_secret_name,
    environment=environment,
    port=port,
    driver=driver,
  )

  os.makedirs(output_dir, exist_ok=True)
  output_path = build_sample_parquet_path(
    database_name=database_name, output_dir=output_dir
  )

  duckdb_connection = duckdb.connect()
  log("(extract_query_sample_to_parquet) carregando extensão ODBC do DuckDB")
  duckdb_connection.execute("LOAD odbc")
  log(f"(extract_query_sample_to_parquet) executando query: {query}")
  duckdb_connection.execute(
    """
    create or replace table sample as
    select *
    from odbc_query(?, ?)
    """,
    [connection_string, query],
  )
  row_count = duckdb_connection.execute("select count(*) from sample").fetchone()[0]
  duckdb_connection.execute(
    f"copy sample to '{output_path}' (format parquet, compression zstd)"
  )
  duckdb_connection.close()

  result = {
    "database_name": database_name,
    "query": query,
    "output_path": output_path,
    "row_count": row_count,
  }
  log(f"(extract_query_sample_to_parquet) resultado: {result}")
  return result
