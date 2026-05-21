# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.datalake.migrate.gcs_to_cloudsql.tasks import start_instance, stop_instance
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import (
  DEFAULT_CLOUDSQL_INSTANCE_CONNECTION_NAME,
  DEFAULT_SAMPLE_QUERY,
  DEFAULT_SQLSERVER_DRIVER,
  DEFAULT_SQLSERVER_PORT,
  LOCAL_CLOUDSQL_PROXY_HOST,
)
from .schedules import schedules
from .tasks import (
  extract_query_sample_to_parquet,
  start_cloudsql_proxy,
  stop_cloudsql_proxy,
  test_sqlserver_connection,
)


@flow(
  name="Migrate: Cloud SQL to BigQuery - Spike",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
  description="Valida conexão SQL Server no Cloud SQL e extração via DuckDB",
)
def cloudsql_to_bigquery_spike(
  instance_name: str,
  database_name: str,
  host_secret_name: str,
  username_secret_name: str,
  password_secret_name: str,
  secret_path: str,
  port_secret_name: str = None,
  cloudsql_instance_connection_name: str = DEFAULT_CLOUDSQL_INSTANCE_CONNECTION_NAME,
  sample_query: str = DEFAULT_SAMPLE_QUERY,
  sqlserver_port: int = DEFAULT_SQLSERVER_PORT,
  sqlserver_driver: str = DEFAULT_SQLSERVER_DRIVER,
  environment: str = "dev",
):
  rename_flow_run(new_name=f"{environment} - {database_name}")
  log(
    f"(cloudsql_to_bigquery_spike) iniciando spike para database "
    f"'{database_name}' na instância '{instance_name}'"
  )

  proxy_process_id = None
  try:
    log(f"(cloudsql_to_bigquery_spike) ligando instância '{instance_name}'")
    start_instance(instance_name=instance_name)

    proxy_process_id = start_cloudsql_proxy(
      instance_connection_name=cloudsql_instance_connection_name, port=sqlserver_port
    )

    log("(cloudsql_to_bigquery_spike) testando conexão SQL Server via pyodbc")
    test_sqlserver_connection(
      host_secret_name=host_secret_name,
      username_secret_name=username_secret_name,
      password_secret_name=password_secret_name,
      secret_path=secret_path,
      database_name=database_name,
      port_secret_name=port_secret_name,
      environment=environment,
      port=sqlserver_port,
      driver=sqlserver_driver,
      host=LOCAL_CLOUDSQL_PROXY_HOST,
    )

    log("(cloudsql_to_bigquery_spike) extraindo amostra via DuckDB para Parquet")
    extract_query_sample_to_parquet(
      host_secret_name=host_secret_name,
      username_secret_name=username_secret_name,
      password_secret_name=password_secret_name,
      secret_path=secret_path,
      database_name=database_name,
      port_secret_name=port_secret_name,
      query=sample_query,
      environment=environment,
      port=sqlserver_port,
      driver=sqlserver_driver,
      host=LOCAL_CLOUDSQL_PROXY_HOST,
    )
    log("(cloudsql_to_bigquery_spike) spike finalizado com sucesso")

  finally:
    if proxy_process_id:
      stop_cloudsql_proxy(process_id=proxy_process_id)

    log(f"(cloudsql_to_bigquery_spike) desligando instância '{instance_name}'")
    stop_instance(instance_name=instance_name)


_flows = [
  flow_config(
    flow=cloudsql_to_bigquery_spike,
    schedules=schedules,
    dockerfile="./pipelines/datalake/migrate/cloudsql_to_bigquery/Dockerfile",
    memory="large",
  )
]
