# -*- coding: utf-8 -*-
from prefect.concurrency.sync import rate_limit
from prefect.futures import wait

from pipelines.constants import constants as global_consts
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import constants as flow_constants
from .schedules import schedules
from .tasks import (
  extract_data,
  generate_endpoint_params,
  get_property_from_dict,
  send_email_notification,
)


@flow(
  name="Extração - Vitacare API",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.PEDRO_ID.value],
)
def vitacare_api(
  environment: str = "dev",
  rename_flow: bool = False,
  target_date: str = "D-1",
  endpoint: str = None,
  dataset_id: str = flow_constants.DATASET_ID.value,
  table_id_prefix: str = None,
):
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

  extracted_data = [
    get_property_from_dict(data=result, key="data") for result in extraction_results
  ]
  logs = [
    get_property_from_dict(data=result, key="logs") for result in extraction_results
  ]

  upload_futures = []
  for df, table_name in zip(extracted_data, table_names):
    rate_limit("um-por-segundo")
    upload_futures.append(
      upload_df_to_datalake_task.submit(
        df=df,
        dataset_id=dataset_id,
        table_id=table_name,
        date_partition_column="_loaded_at",
        source_format="parquet",
        dump_mode="append",
      )
    )
  wait(upload_futures)

  send_email_notification(
    logs=logs, endpoint=endpoint, environment=environment, target_date=parsed_target_date
  )


_flows = [flow_config(flow=vitacare_api, schedules=schedules)]
