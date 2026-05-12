# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.prefect import create_flow_run, flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules
from .tasks import clone_bigquery_table


@flow(
  name="DataLake - Extração e Carga de Dados - Clonagem de BigQuery",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.CIT_ID.value],
  description="Clona dataset de projeto externo no BigQuery para o nosso datalake",
)
def clone_bigquery(
  source_project_name: str,  # ex. "rj-smfp"
  source_dataset_name: str,
  source_table_list: list[str],
  destination_dataset_name: str,
  dbt_select_exp: str = None,
  environment: str = "dev",
):
  """
  Args:
    source_project_name (str):
      Nome do projeto no BigQuery fonte (ex. "rj-smfp")
    source_dataset_name (str):
      Nome do dataset fonte
    source_table_list (list[str]):
      Lista da tabelas fonte a serem clonadas
    destination_dataset_name (str):
      Nome do dataset de destino
    dbt_select_exp (str?):
      Expressão a ser usada após um `--select` de dbt. Se presente,
      é executada após a clonagem das tabelas.
      Cria uma nova flow run de dbt.
    environment: (str?):
      Ambiente de execução; "dev" por padrão.
  """
  bigquery_project = get_google_project_for_environment(environment=environment)
  rename_task = rename_flow_run(
    new_name=f"Cloning dataset '{source_project_name}.{source_dataset_name}' into '{bigquery_project}'"
  )

  clone_table_task = clone_bigquery_table(
    source_project_name=source_project_name,
    source_dataset_name=source_dataset_name,
    source_table_list=source_table_list,
    destination_project_name=bigquery_project,
    destination_dataset_name=destination_dataset_name,
    wait_for=[rename_task],
  )

  if dbt_select_exp:
    create_flow_run(
      flow_name="DataLake - Transformação - DBT",
      environment=environment,
      parameters={
        "command": "run",
        "select": dbt_select_exp,
        "environment": environment,
        "rename_flow": True,
        "send_discord_report": False,
      },
      wait_for=[clone_table_task],
    )


# num_workers=1, memory_limit="2Gi"
_flows = [flow_config(flow=clone_bigquery, schedules=schedules)]
