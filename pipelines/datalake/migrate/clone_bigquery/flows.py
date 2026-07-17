# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.datalake.transform.dbt.flows import sms_execute_dbt
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.prefect import create_flow_run, flow, flow_config, rename_flow_run

from .schedules import schedules
from .tasks import clone_bigquery_table, download_then_reupload_bigquery_table


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
  horribly_inefficient_method: bool = False,
  horribly_inefficient_chunk_size: int = 100_000,
  dbt_select_exp: str | None = None,
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
    horribly_inefficient_method (bool?):
      Em alguns casos, não é possível fazer nem um `CLONE` nem um
      `CREATE ... AS SELECT`, mesmo com permissão de leitura à tabela fonte.
      Nesses casos, é necessário usar o método horrivelmente
      ineficiente: `SELECT *`, salva como parquet, reupload como tabela.
    horribly_inefficient_chunk_size (int?):
      Nos casos ineficientes acima, as tabelas são extraídas em pedaços
      de `horribly_inefficient_chunk_size`; por padrão, 100,000.
    environment (str?):
      Ambiente de execução; "dev" por padrão.
  """
  bigquery_project = get_google_project_for_environment(environment=environment)
  rename_flow_run(
    new_name=f"Cloning dataset '{source_project_name}.{source_dataset_name}' into '{bigquery_project}'"
  )

  if not horribly_inefficient_method:
    # Sem .submit(), essa task é bloqueante, então só segue pro `if` abaixo quando termina
    clone_bigquery_table(
      source_project_name=source_project_name,
      source_dataset_name=source_dataset_name,
      source_table_list=source_table_list,
      destination_project_name=bigquery_project,
      destination_dataset_name=destination_dataset_name,
    )
  else:
    # Método horrivelmente ineficiente: baixa cada tabela individualmente
    # para dataframes, e chama `upload_df_to_datalake()` pra cada pedaço
    for table in source_table_list:
      download_then_reupload_bigquery_table(
        source_project_name=source_project_name,
        source_dataset_name=source_dataset_name,
        source_table_name=table,
        destination_dataset_name=destination_dataset_name,
        chunk_size=horribly_inefficient_chunk_size,
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
