# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.datalake import upload_to_datalake_task
from pipelines.utils.io import create_data_folders_task, create_partitions_task
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import constants as tpc_constants
from .schedules import schedules
from .tasks import extract_data_from_blob, transform_data


@flow(
  name="DataLake - Extração e Carga de Dados - TPC",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def sms_dump_tpc(
  blob_file: str,
  table_id: str,
  dataset_id: str = tpc_constants.DATASET_ID.value,
  rename_flow: bool = False,
  environment: str = "dev",
):
  if rename_flow:
    rename_flow_run(new_name=f"TPC: {environment} - {blob_file}")

  local_folders = create_data_folders_task()

  raw_file = extract_data_from_blob(
    blob_file=blob_file,
    file_folder=local_folders["raw"],
    file_name=table_id,
    environment=environment,
    wait_for=[local_folders],
  )

  transformed_file = transform_data(file_path=raw_file, blob_file=blob_file)

  partitions = create_partitions_task(
    data_path=local_folders["raw"],
    partition_directory=local_folders["partition_directory"],
    wait_for=[transformed_file],
  )

  upload_to_datalake_task(
    input_path=local_folders["partition_directory"],
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode="replace",
    delete_mode="staging",
    csv_delimiter=";",
    if_storage_data_exists="replace",
    biglake_table=True,
    dataset_is_public=False,
    wait_for=[partitions],
  )


_flows = [flow_config(flow=sms_dump_tpc, schedules=schedules)]
