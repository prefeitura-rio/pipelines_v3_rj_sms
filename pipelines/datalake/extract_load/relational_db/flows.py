# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts

from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import download_from_db
from .schedules import schedules


@flow(
  name="DataLake - Extração e Carga de Dados - Banco de Dados Relacional",
  state_handlers=[handle_flow_state_change],
  owners=[
    global_consts.PEDRO_ID.value,
  ],
)
def extract_load_relational_db(
  db_url_infisical_key: str,
  db_url_infisical_path: str,

  target_dataset_id: str,
  target_table_id: str,

  source_schema_name: str,
  source_table_name: str,
  source_datetime_column: str = "created_at",

  relative_date: str = "D-1",
  historical_mode: bool = False,

  rename_flow: bool = True,
  environment: str = "dev",
):
  if rename_flow:
    rename_flow_run(new_name=f"({environment}) '{target_dataset_id}.{target_table_id}'")

  database_url = get_secret(
    secret_name=db_url_infisical_key,
    path=db_url_infisical_path,
    environment=environment,
  )

  dataframe = download_from_db(
    db_url=database_url,
    db_table=source_table_name,
    db_schema=source_schema_name,
    relative_date=relative_date,
    historical_mode=historical_mode,
    reference_datetime_column=source_datetime_column,
  )

  upload_df_to_datalake(
    df=dataframe,
    dataset_id=target_dataset_id,
    table_id=f"{source_schema_name}__{source_table_name}",
    source_format="parquet",
    partition_column="loaded_at",
  )


# memory_limit="8Gi", num_workers=2
_flows = [flow_config(flow=extract_load_relational_db, schedules=schedules)]
