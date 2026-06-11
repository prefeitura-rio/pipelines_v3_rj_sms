# -*- coding: utf-8 -*-
from typing import Literal

from pipelines.constants import CIT
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import constants
from .schedules import schedules
from .tasks import baixar_endpoint, file_to_dataframe, login


@flow(
  name="Extração: Sisreg Web",
  description="Flow de extração de escalas profissionais do site do Sisreg",
  owners=[CIT.AVELLAR_ID.value],
  state_handlers=[handle_flow_state_change],
)
def extract_sisreg_web(
  endpoint: Literal["escala"] = "escala",
  dataset_id: str = "brutos_sisreg",
  table_id: str = "escala",
  environment: str = "dev",
):
  rename_flow_run(
    new_name=f"Extração {endpoint} -> '{dataset_id}.{table_id}' ({environment})"
  )

  USERNAME = get_secret(
    secret_name=constants.INFISICAL_USERNAME.value,
    path=constants.INFISICAL_PATH.value,
    environment=environment,
  )
  PASSWORD = get_secret(
    secret_name=constants.INFISICAL_PASSWORD.value,
    path=constants.INFISICAL_PATH.value,
    environment=environment,
  )

  cookies = login(username=USERNAME, password=PASSWORD)
  csv_path = baixar_endpoint(endpoint=endpoint, cookies=cookies)
  df = file_to_dataframe(file_path=csv_path)
  upload_df_to_datalake_task(
    df=df,
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode="append",
    source_format="parquet",
    date_partition_column="data_particao",
    dataset_is_public=False,
  )


_flows = [flow_config(flow=extract_sisreg_web, schedules=schedules, region="bra")]
