# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import vitacare_constants
from .schedules import schedules
from .tasks import (
  extract_table_to_bigquery,
  get_cnes_from_bigquery,
  get_database_tables,
  start_cloudsql_proxy,
  start_cloudsql_instance,
  stop_cloudsql_proxy,
  stop_cloudsql_instance,
  validate_environment,
  write_log,
)


@flow(
  name="Extração: Vitacare Histórico",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def vitacare_historico(
  environment: str = "dev", cnes: str = None, table_name: str = None
):
  environment = validate_environment(environment=environment)
  rename_flow_run(new_name=f"{environment} - vitacare_historico")
  database_environment = "dev"

  cnes_list = [cnes] if cnes else get_cnes_from_bigquery()
  table_names = (
    [table_name] if table_name else get_database_tables(environment=environment)
  )
  project_id = get_google_project_for_environment(environment=environment)
  log_table_id = (
    f"{project_id}.{vitacare_constants.LOG_DATASET.value}."
    f"{vitacare_constants.LOG_TABLE.value}"
  )

  proxy_process_id = None
  instance_started = False
  results = []

  try:
    start_cloudsql_instance()
    instance_started = True
    proxy_process_id = start_cloudsql_proxy(environment=database_environment)

    for cnes in cnes_list:
      database_name = f"vitacare_historic_{cnes}"

      for table_name in table_names:
        result = extract_table_to_bigquery(
          database_name=database_name,
          environment=database_environment,
          cnes=cnes,
          table_name=table_name,
        )
        results.append(result)

  finally:
    if proxy_process_id:
      stop_cloudsql_proxy(process_id=proxy_process_id)
    if instance_started:
      stop_cloudsql_instance()
    if results:
      write_log(log_items=results, log_table_id=log_table_id)

  total_failed = sum(1 for result in results if result["status"] == "failed")

  if total_failed > 0:
    raise RuntimeError(f"{total_failed} extração(ões) falharam")


_flows = [
  flow_config(
    flow=vitacare_historico,
    schedules=schedules,
    dockerfile="./pipelines/datalake/extract_load/vitacare_historico/Dockerfile",
    memory="medium",
  )
]
