# -*- coding: utf-8 -*-
import os
import socket
import subprocess
import time

from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import (
  DEFAULT_CLOUDSQL_INSTANCE_CONNECTION_NAME,
  DEFAULT_SAMPLE_QUERY,
  DEFAULT_SQLSERVER_DRIVER,
  DEFAULT_SQLSERVER_PORT,
  LOCAL_CLOUDSQL_PROXY_HOST,
)
from .utils import (
  build_sample_parquet_path,
  build_sqlserver_odbc_connection_string,
  get_host_and_port_from_odbc_connection_string,
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
  host: str = None,
) -> str:
  log(
    f"(get_sqlserver_connection_string) buscando secrets em '{secret_path}' "
    f"para database '{database_name}'"
  )
  if not host:
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
def start_cloudsql_proxy(
  instance_connection_name: str = DEFAULT_CLOUDSQL_INSTANCE_CONNECTION_NAME,
  port: int = DEFAULT_SQLSERVER_PORT,
) -> int:
  command = [
    "cloud-sql-proxy",
    f"--address={LOCAL_CLOUDSQL_PROXY_HOST}",
    f"--port={port}",
    instance_connection_name,
  ]
  log(f"(start_cloudsql_proxy) iniciando proxy: {' '.join(command)}")
  process = subprocess.Popen(command)

  deadline = time.monotonic() + 30
  while time.monotonic() < deadline:
    if process.poll() is not None:
      raise RuntimeError(
        "Cloud SQL Auth Proxy finalizou antes de abrir conexão. "
        f"Exit code: {process.returncode}"
      )

    try:
      with socket.create_connection((LOCAL_CLOUDSQL_PROXY_HOST, port), timeout=1):
        log(
          f"(start_cloudsql_proxy) proxy disponível em {LOCAL_CLOUDSQL_PROXY_HOST}:{port}"
        )
        return process.pid
    except OSError:
      time.sleep(1)

  process.terminate()
  raise TimeoutError("Cloud SQL Auth Proxy não abriu conexão em até 30 segundos")


@task
def stop_cloudsql_proxy(process_id: int) -> None:
  if not process_id:
    return

  log(f"(stop_cloudsql_proxy) encerrando processo {process_id}")
  subprocess.run(["kill", str(process_id)], check=False)


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
  host: str = None,
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
    host=host,
  )
  host, resolved_port = get_host_and_port_from_odbc_connection_string(connection_string)
  log(f"(test_sqlserver_connection) resolvendo host '{host}'")
  addresses = socket.getaddrinfo(host, resolved_port, proto=socket.IPPROTO_TCP)
  resolved_addresses = sorted({address[4][0] for address in addresses})
  log(
    f"(test_sqlserver_connection) host '{host}' resolveu para "
    f"{resolved_addresses}; testando TCP na porta {resolved_port}"
  )
  with socket.create_connection((host, resolved_port), timeout=10):
    log(f"(test_sqlserver_connection) conexão TCP com '{host}:{resolved_port}' OK")

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
  host: str = None,
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
    host=host,
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
