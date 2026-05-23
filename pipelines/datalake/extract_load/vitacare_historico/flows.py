# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor, as_completed

from pipelines.constants import CIT
from pipelines.utils.datetime import now_str
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.logger import log
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

    def extract_cnes_tables(cnes_item: str) -> list[dict]:
      cnes_results = []
      log(
        f"(vitacare_historico) iniciando CNES '{cnes_item}' com "
        f"{len(table_names)} tabela(s)"
      )
      for table_name in table_names:
        cnes_results.append(
          extract_table_to_bigquery.fn(
            database_name=f"vitacare_historic_{cnes_item}",
            environment=database_environment,
            cnes=cnes_item,
            table_name=table_name,
          )
        )
      log(f"(vitacare_historico) CNES '{cnes_item}' finalizado")
      return cnes_results

    with ThreadPoolExecutor(
      max_workers=vitacare_constants.CNES_CONCURRENCY_LIMIT.value
    ) as executor:
      future_to_cnes = {
        executor.submit(extract_cnes_tables, cnes_item): cnes_item
        for cnes_item in cnes_list
      }
      log(
        f"(vitacare_historico) {len(future_to_cnes)} CNES submetido(s), "
        f"limite de concorrência {vitacare_constants.CNES_CONCURRENCY_LIMIT.value}"
      )

      for future in as_completed(future_to_cnes):
        cnes_item = future_to_cnes[future]
        try:
          results.extend(future.result())
        except Exception as exc:  # pylint: disable=broad-except
          log(
            f"(vitacare_historico) erro inesperado processando CNES "
            f"'{cnes_item}': {repr(exc)}",
            level="error",
          )
          raise

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
