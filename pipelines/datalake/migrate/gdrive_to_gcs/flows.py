# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.google import build_bucket_name, list_google_drive_files
from pipelines.utils.prefect import flow, flow_config

from .tasks import process_google_drive_file
from .utils import build_execution_summary


@flow(
  name="Migração: Google Drive → GCS",
  description="Lista arquivos do Google Drive e faz upload para o GCS",
  owners=[CIT.CIT_ID.value],
  tags=["CIT"],
)
def gdrive_to_gcs(
  root_folder_id: str,
  bucket_name: str,
  environment: str = "dev",
  last_modified_date: str = "M-0",
  last_modified_end_date: str = "D-0",
):
  resolved_bucket_name = build_bucket_name(
    bucket_name=bucket_name, environment=environment
  )

  items = list_google_drive_files(
    folder_id=root_folder_id,
    last_modified_date=last_modified_date,
    last_modified_end_date=last_modified_end_date,
  )

  results = []
  for item in items:
    results.append(process_google_drive_file(item=item, bucket_name=resolved_bucket_name))

  return build_execution_summary(results)


_flows = [flow_config(flow=gdrive_to_gcs)]
