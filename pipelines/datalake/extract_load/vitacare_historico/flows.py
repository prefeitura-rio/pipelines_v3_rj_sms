# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules
from .tasks import (
  extract_table_to_bigquery,
  get_cnes_from_bigquery,
  get_database_engine,
  get_database_tables,
  start_cloudsql_proxy,
  stop_cloudsql_proxy,
)


@flow(
  name="Extracao: Vitacare Historico",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def extract_load_vitacare_historico(
  environment: str = "dev", cnes: str = None, table_name: str = None
):
  rename_flow_run(new_name=f"{environment} - vitacare_historico")

  cnes_list = [cnes] if cnes else get_cnes_from_bigquery()
  table_names = (
    [table_name] if table_name else get_database_tables(environment=environment)
  )
  log(f"Plano de extração: {len(cnes_list)} CNES, tabelas={table_names}")
  proxy_process_id = None
  results = []

  try:
    proxy_process_id = start_cloudsql_proxy(environment=environment)

    for cnes in cnes_list:
      database_name = f"vitacare_historic_{cnes}"
      log(f"Extraindo database '{database_name}'")

      engine = get_database_engine.fn(
        database_name=database_name, environment=environment
      )

      for table_name in table_names:
        result = extract_table_to_bigquery(
          engine=engine, cnes=cnes, table_name=table_name
        )
        results.append(result)

  finally:
    if proxy_process_id:
      stop_cloudsql_proxy(process_id=proxy_process_id)

  total_rows = sum(result["rows"] for result in results)
  log(
    f"Extração Vitacare Histórico finalizada: "
    f"{len(results)} tabela(s), {total_rows} linha(s)"
  )


_flows = [
  flow_config(
    flow=extract_load_vitacare_historico,
    schedules=schedules,
    dockerfile="./pipelines/datalake/extract_load/vitacare_historico/Dockerfile",
    memory="large",
  )
]
