# -*- coding: utf-8 -*-
from collections import deque

from prefect.futures import as_completed
from prefect.task_runners import ThreadPoolTaskRunner

from pipelines.constants import CIT
from pipelines.utils.datetime import now_str
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import vitacare_constants
from .schedules import schedules
from .tasks import (
  extract_table_to_bigquery,
  get_cnes_from_bigquery,
  get_database_tables,
  start_cloudsql_instance,
  start_cloudsql_proxy,
  stop_cloudsql_instance,
  stop_cloudsql_proxy,
  validate_environment,
  write_log,
)


@flow(
  name="Extração: Vitacare Histórico",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
  task_runner=ThreadPoolTaskRunner(
    max_workers=vitacare_constants.CNES_CONCURRENCY_LIMIT.value
  ),
)
def vitacare_historico(
  environment: str = "dev", cnes: str = None, table_name: str = None
):
  environment = validate_environment(environment=environment)
  rename_flow_run(new_name=f"{environment} - {now_str()}")
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

    if table_names:
      pending_cnes = deque(cnes_list)
      active_futures = {}

      def submit_table(cnes_item: str, table_index: int) -> None:
        table_name = table_names[table_index]
        future = extract_table_to_bigquery.submit(
          database_name=f"vitacare_historic_{cnes_item}",
          environment=database_environment,
          cnes=cnes_item,
          table_name=table_name,
        )
        active_futures[future] = (cnes_item, table_index)

      while (
        pending_cnes
        and len(active_futures) < vitacare_constants.CNES_CONCURRENCY_LIMIT.value
      ):
        submit_table(cnes_item=pending_cnes.popleft(), table_index=0)

      while active_futures:
        for future in as_completed(list(active_futures)):
          cnes_item, table_index = active_futures.pop(future)
          results.append(future.result())

          next_table_index = table_index + 1
          if next_table_index < len(table_names):
            submit_table(cnes_item=cnes_item, table_index=next_table_index)
          elif pending_cnes:
            submit_table(cnes_item=pending_cnes.popleft(), table_index=0)
          break

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
