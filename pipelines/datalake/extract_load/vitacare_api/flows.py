# -*- coding: utf-8 -*-
from prefect.concurrency.sync import rate_limit
from prefect.futures import wait

from pipelines.constants import CIT
from pipelines.datalake.extract_load.vitacare_api.constants import (
  constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_api.schedules import schedules
from pipelines.datalake.extract_load.vitacare_api.tasks import (
  extract_data,
  generate_endpoint_params,
  send_email_notification,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change


@flow(
  name="Extração: Vitacare API",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.PEDRO_ID.value],
)
def vitacare_api(
  environment: str = "dev",
  rename_flow: bool = False,
  skip_email: bool = False,
  target_date: str = "D-1",
  endpoint: str = None,
  dataset_id: str = vitacare_constants.DATASET_ID.value,
  table_id_prefix: str = None,
):
  if endpoint not in vitacare_constants.ENDPOINT.value:
    raise ValueError(
      f"endpoint '{endpoint}' invalido. Valores permitidos: "
      f"{sorted(vitacare_constants.ENDPOINT.value.keys())}"
    )
  if not table_id_prefix:
    raise ValueError("table_id_prefix precisa ser informado")

  parsed_target_date = from_relative_date(relative_date=target_date)

  if rename_flow:
    rename_flow_run(
      new_name=(
        f"environment={environment}, endpoint={endpoint}, "
        f"target_date={parsed_target_date}"
      )
    )

  endpoint_params, table_names = generate_endpoint_params(
    target_date=parsed_target_date,
    environment=environment,
    table_id_prefix=table_id_prefix,
  )

  extraction_futures = []
  for params in endpoint_params:
    rate_limit("um-por-segundo")
    extraction_futures.append(
      extract_data.submit(
        endpoint_params=params, endpoint_name=endpoint, environment=environment
      )
    )

  wait(extraction_futures)
  extraction_results = [future.result() for future in extraction_futures]

  upload_futures = []
  for extraction_result, table_name in zip(extraction_results, table_names):
    rate_limit("um-por-segundo")
    upload_futures.append(
      upload_df_to_datalake_task.submit(
        df=extraction_result["data"],
        dataset_id=dataset_id,
        table_id=table_name,
        date_partition_column="_loaded_at",
        source_format="parquet",
        dump_mode="append",
      )
    )

  wait(upload_futures)

  if not skip_email:
    send_email_notification(
      logs=[result["logs"] for result in extraction_results],
      endpoint=endpoint,
      environment=environment,
      target_date=parsed_target_date,
    )


_flows = [flow_config(flow=vitacare_api, schedules=schedules)]
