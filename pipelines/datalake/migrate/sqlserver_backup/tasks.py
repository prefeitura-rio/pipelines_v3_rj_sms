# -*- coding: utf-8 -*-
import fnmatch

from google.cloud import storage
from prefect.context import FlowRunContext

from pipelines.utils.datalake import update_logs_to_datalake
from pipelines.utils.datetime import now, parse_date_or_today
from pipelines.utils.env import get_prefect_url
from pipelines.utils.google import (
  delete_database,
  dissect_gcs_uri,
  ensure_instance_running,
  ensure_instance_stopped,
  import_backup_to_database,
  wait_for_operations,
)
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .utils import build_restore_plan_for_backup_type


@task(retries=3, retry_delay_seconds=30)
def list_backup_files(bucket_name: str, file_pattern: str) -> list[str]:
  client = storage.Client()
  bucket = client.bucket(bucket_name)

  files = [
    blob.name for blob in bucket.list_blobs() if fnmatch.fnmatch(blob.name, file_pattern)
  ]

  log(f"(list_backup_files) encontrados {len(files)} arquivo(s)")
  return files


@task
def prepare_restore_plan(
  files: list[str], bucket_name: str, backup_type: str
) -> list[dict]:
  plan = build_restore_plan_for_backup_type(
    files=files, bucket_name=bucket_name, backup_type=backup_type
  )
  plan = sorted(plan, key=lambda item: item["gcs_uri"])
  log(f"(prepare_restore_plan) plano com {len(plan)} restauração(ões)")
  return plan


@task
def start_instance(instance_name: str) -> None:
  wait_for_operations(instance_name=instance_name)
  ensure_instance_running(instance_name=instance_name)


@task
def stop_instance(instance_name: str) -> None:
  wait_for_operations(instance_name=instance_name)
  ensure_instance_stopped(instance_name=instance_name)


@task
def restore_backup(restore_item: dict, instance_name: str) -> dict:
  started_at = now()
  gcs_uri = restore_item["gcs_uri"]
  database_name = restore_item["database_name"]

  log(
    f"(restore_backup) restaurando '{gcs_uri}' na database "
    f"'{database_name}' da instância '{instance_name}'"
  )

  result = {
    "gcs_uri": gcs_uri,
    "instance_name": instance_name,
    "database_name": database_name,
    "status": "success",
    "error_message": None,
    "started_at": started_at.isoformat(),
    "finished_at": None,
  }

  try:
    uri = dissect_gcs_uri(gcs_uri)
    if uri["file_ext"].lower() != "bak":
      raise ValueError(f"Arquivo '{gcs_uri}' não é um .bak")

    wait_for_operations(instance_name=instance_name)

    delete_database(instance_name=instance_name, database_name=database_name)
    wait_for_operations(instance_name=instance_name)

    import_backup_to_database(
      instance_name=instance_name, source_uri=gcs_uri, database_name=database_name
    )
    wait_for_operations(instance_name=instance_name)

  except Exception as exc:
    result["status"] = "failed"
    result["error_message"] = str(exc)
    log(f"(restore_backup) erro restaurando '{gcs_uri}': {repr(exc)}", level="error")

  finally:
    result["finished_at"] = now().isoformat()

  return result


@task
def write_log(
  log_items: list[dict], dataset_id: str, table_id: str, environment: str
) -> dict:
  if not log_items:
    return {"inserted_rows": 0}

  flow_run_id = None
  flow_run_url = None
  flow_run_context = FlowRunContext.get()

  if flow_run_context:
    flow_run_id = str(flow_run_context.flow_run.id)
    flow_run_url = f"{get_prefect_url()}/runs/flow-run/{flow_run_id}"

  fallback_timestamp = now().isoformat()

  rows = []
  for log_item in log_items:
    timestamp = log_item.get("finished_at") or fallback_timestamp
    rows.append(
      {
        "flow_run_id": flow_run_id,
        "flow_run_url": flow_run_url,
        "gcs_uri": log_item["gcs_uri"],
        "instance_name": log_item["instance_name"],
        "database_name": log_item["database_name"],
        "status": log_item["status"],
        "error_message": log_item["error_message"],
        "started_at": log_item["started_at"],
        "finished_at": log_item["finished_at"],
        "timestamp": timestamp,
        "data_particao": parse_date_or_today(timestamp).date().isoformat(),
      }
    )

  return update_logs_to_datalake(
    logs=rows, dataset_id=dataset_id, table_id=table_id, environment=environment
  )
