# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.datalake.transform.dbt.flows import sms_execute_dbt
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.prefect import create_flow_run, flow, flow_config, rename_flow_run

from .schedules import schedules
from .tasks import clone_bigquery_table


@flow(
  name="Migração: Clona BigQuery",
  description="Clona dataset de projeto no BigQuery para o nosso datalake",
  owners=[CIT.CIT_ID.value],
  tags=["CIT"],
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

  # Sem .submit(), em teoria essa chamada é bloqueante
  # então só segue pro `if` abaixo quando termina
  clone_bigquery_table(
    source_project_name=source_project_name,
    source_dataset_name=source_dataset_name,
    source_table_list=source_table_list,
    destination_project_name=bigquery_project,
    destination_dataset_name=destination_dataset_name,
    wait_for=[rename_task],
  )

  if dbt_select_exp:
    create_flow_run(
      flow=sms_execute_dbt,
      environment=environment,
      parameters={
        "command": "run",
        "select": dbt_select_exp,
        "environment": environment,
        "rename_flow": True,
        "send_discord_report": False,
      },
    )


# num_workers=1, memory_limit="2Gi"
_flows = [flow_config(flow=clone_bigquery, schedules=schedules)]
