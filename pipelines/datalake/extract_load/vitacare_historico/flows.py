# -*- coding: utf-8 -*-
from prefect.futures import wait

from pipelines.constants import CIT
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import vitacare_constants
from .schedules import schedules
from .tasks import (
  ensure_cnes_concurrency_limit,
  extract_cnes_tables,
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
  cnes_futures = []
  flow_stage = "before_cloudsql"

  try:
    log(
      f"(vitacare_historico) preparando extração de {len(cnes_list)} CNES "
      f"e {len(table_names)} tabela(s)"
    )
    ensure_cnes_concurrency_limit()
    flow_stage = "starting_cloudsql"
    start_cloudsql_instance()
    instance_started = True
    proxy_process_id = start_cloudsql_proxy(environment=database_environment)

    flow_stage = "submitting_cnes"
    for index, cnes in enumerate(cnes_list, start=1):
      try:
        future = extract_cnes_tables.submit(
          environment=database_environment, cnes=cnes, table_names=table_names
        )
        cnes_futures.append(future)
        log(
          f"(vitacare_historico) CNES '{cnes}' submetido "
          f"({index}/{len(cnes_list)}), task_run_id={future.task_run_id}"
        )
      except Exception as exc:
        log(
          f"(vitacare_historico) erro ao submeter CNES '{cnes}' "
          f"({index}/{len(cnes_list)}); {len(cnes_futures)} future(s) "
          f"já submetido(s). Erro original: {repr(exc)}",
          level="error",
        )
        raise

    flow_stage = "waiting_cnes"
    log(f"(vitacare_historico) aguardando {len(cnes_futures)} future(s) de CNES")
    pending_futures = list(cnes_futures)
    while pending_futures:
      done_futures, not_done_futures = wait(pending_futures, timeout=60)
      log(
        f"(vitacare_historico) wait parcial: {len(done_futures)} concluído(s), "
        f"{len(not_done_futures)} pendente(s)"
      )

      if not done_futures and not_done_futures:
        pending_ids = [str(future.task_run_id) for future in list(not_done_futures)[:10]]
        log(
          f"(vitacare_historico) futures ainda pendentes "
          f"(primeiros 10 task_run_id): {pending_ids}",
          level="warning",
        )

      pending_futures = list(not_done_futures)

    flow_stage = "collecting_results"
    for future in cnes_futures:
      log(
        f"(vitacare_historico) coletando resultado do future "
        f"task_run_id={future.task_run_id}"
      )
      results.extend(future.result())

  finally:
    log(
      f"(vitacare_historico) entrando no cleanup; etapa='{flow_stage}', "
      f"future(s) submetido(s)={len(cnes_futures)}, resultado(s)={len(results)}"
    )
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
