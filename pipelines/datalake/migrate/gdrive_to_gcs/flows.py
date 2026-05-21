# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.google import build_bucket_name
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules
from .tasks import (
  cleanup_downloaded_file,
  download_file,
  list_files,
  prepare_files_for_upload,
  upload_file,
  write_log,
)

LOG_DATASET_ID = "controle_pipelines"


@flow(
  name="Migrate - Google Drive to GCS",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
  description="Lista arquivos do Google Drive e envia para o GCS",
)
def gdrive_to_gcs(
  root_folder_id: str,
  bucket_name: str,
  table_id: str,
  start_date: str = None,
  end_date: str = None,
  environment: str = "dev",
):
  rename_flow_run(new_name=f"{environment} - {bucket_name}")

  resolved_bucket_name = build_bucket_name(
    bucket_name=bucket_name, environment=environment
  )
  project_id = get_google_project_for_environment(environment=environment)
  log_table_id = f"{project_id}.{LOG_DATASET_ID}.{table_id}"

  files = []
  log_items = []
  start_date = from_relative_date(start_date) if start_date else None
  end_date = from_relative_date(end_date) if end_date else None

  try:
    files = list_files(folder_id=root_folder_id, start_date=start_date, end_date=end_date)

    # Processamento sequencial para evitar muitos downloads/uploads simultâneos.
    for file in files:
      downloaded_file = download_file(file=file)
      prepared_files = prepare_files_for_upload(downloaded_file=downloaded_file)

      for prepared_file in prepared_files:
        result = upload_file(
          prepared_file=prepared_file, bucket_name=resolved_bucket_name
        )
        log_items.append(result)

      cleanup_downloaded_file(downloaded_file=downloaded_file)

  finally:
    if log_items:
      write_log(log_items=log_items, log_table_id=log_table_id)

  total_success = sum(1 for log_item in log_items if log_item["status"] == "success")
  total_failed = sum(1 for log_item in log_items if log_item["status"] == "failed")

  log(
    f"(gdrive_to_gcs) processamento finalizado: "
    f"{total_success} sucesso(s), {total_failed} falha(s)"
  )


_flows = [flow_config(flow=gdrive_to_gcs, schedules=schedules)]
