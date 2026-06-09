# -*- coding: utf-8 -*-
from typing import Literal

from pipelines.constants import CIT
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import baixar_endpoint, file_to_dataframe, login
from .constants import constants


@flow(
  name="Extração: SISREG Web",
  owners=[CIT.AVELLAR_ID.value],
  state_handlers=[handle_flow_state_change],
)
def extract_sisreg_web(endpoint: Literal["escala"] = "escala", environment: str = "dev"):
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
    dataset_id="",
    table_id="",
    dump_mode="replace",
    source_format="parquet",
    dataset_is_public=False,
  )


_flows = [flow_config(flow=extract_sisreg_web, schedules=[])]
