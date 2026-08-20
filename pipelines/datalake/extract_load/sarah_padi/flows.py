# -*- coding: utf-8 -*-
from typing import Literal, Optional

from pipelines.constants import CIT
from pipelines.datalake.extract_load.sarah_padi.constants import (
  constants as padi_constants,
)
from pipelines.datalake.extract_load.sarah_padi.tasks import auth, get_fatos, parse_date
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow


@flow(name="Extração: SARAH PADI", owners=[CIT.HERIAN_ID.value], tags=["CIT"])
def padi_extraction(
  tabela: Literal[padi_constants.TABLES.value.keys()],
  date: Optional[str],
  dataset_id: str = "brutos_prontuario_sarah_padi",
  environment: str = "dev",
):

  USER = get_secret(
    path=padi_constants.INFISICAL_PATH.value, secret_name="user", environment=environment
  )
  PASSWORD = get_secret(
    path=padi_constants.INFISICAL_PATH.value,
    secret_name="password",
    environment=environment,
  )
  ACCESS_TOKEN = get_secret(
    path=padi_constants.INFISICAL_PATH.value,
    secret_name="access-token",
    environment=environment,
  )
  AUTH_URL = get_secret(
    path=padi_constants.INFISICAL_PATH.value,
    secret_name="auth-url",
    environment=environment,
  )
  BIS_URL = get_secret(
    path=padi_constants.INFISICAL_PATH.value,
    secret_name="bis-url",
    environment=environment,
  )

  token = auth(url=AUTH_URL, user=USER, password=PASSWORD)

  data_str = parse_date(date_str=date)

  df = get_fatos(
    url=BIS_URL,
    cnes=padi_constants.CNES.value,
    tabela=tabela,
    data=data_str,
    access_token=ACCESS_TOKEN,
    token=token,
  )

  upload_df_to_datalake_task(
    df=df,
    dataset_id=dataset_id,
    table_id=tabela,
    dump_mode="append",
    date_partition_column="extracted_at",
    csv_delimiter=";",
  )


_flows = [flow_config(flow=padi_extraction)]
