# -*- coding: utf-8 -*-
from prefect.futures import wait

from pipelines.constants import CIT
from pipelines.datalake.extract_load.vitacare_api_v2.constants import (
  constants as flow_constants,
)
from pipelines.datalake.extract_load.vitacare_api_v2.tasks import (
  extract_data,
  generate_endpoint_params,
  send_email_notification,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.google_cloud import load_file_from_bigquery
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change


@flow(
  name="DataLake - Extração e Carga de Dados - VitaCare API v2",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.PEDRO_ID.value],
)
def sms_vitacare_api_v2(
  environment: str = "dev",
  rename_flow: bool = False,
  target_date: str = "D-1",
  endpoint: str = None,
  dataset_id: str = flow_constants.DATASET_ID.value,
  table_id_prefix: str = None,
):
  if endpoint not in flow_constants.ENDPOINT.value:
    allowed = ", ".join(flow_constants.ENDPOINT.value.keys())
    raise ValueError(f"Endpoint '{endpoint}' inválido. Opções: {allowed}")

  resolved_target_date = from_relative_date(relative_date=target_date)
  resolved_target_date_str = resolved_target_date.strftime("%Y-%m-%d")

  if rename_flow:
    rename_flow_run(new_name=f"{endpoint} - {resolved_target_date_str} - {environment}")

  username = get_secret(
    secret_name=flow_constants.INFISICAL_VITACARE_USERNAME.value,
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  password = get_secret(
    secret_name=flow_constants.INFISICAL_VITACARE_PASSWORD.value,
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  estabelecimentos = load_file_from_bigquery(
    project_name="rj-sms",
    dataset_name="saude_dados_mestres",
    table_name="estabelecimento",
    environment=environment,
  )

  endpoint_params, table_names = generate_endpoint_params(
    estabelecimentos=estabelecimentos,
    target_date=resolved_target_date,
    username=username,
    password=password,
    table_id_prefix=table_id_prefix,
  )

  extraction_futures = [
    extract_data.submit(
      endpoint_params=params, endpoint_name=endpoint, environment=environment
    )
    for params in endpoint_params
  ]
  wait(extraction_futures)
  extraction_results = [future.result() for future in extraction_futures]

  upload_futures = []
  for result, table_name in zip(extraction_results, table_names, strict=True):
    upload_futures.append(
      upload_df_to_datalake_task.submit(
        df=result["data"],
        dataset_id=dataset_id,
        table_id=table_name,
        dump_mode="append",
        source_format="parquet",
        date_partition_column="_loaded_at",
      )
    )

  wait(upload_futures)

  send_email_notification(
    logs=[result["logs"] for result in extraction_results],
    endpoint=endpoint,
    environment=environment,
    target_date=resolved_target_date_str,
  )


_flows = [flow_config(flow=sms_vitacare_api_v2, memory="small")]
