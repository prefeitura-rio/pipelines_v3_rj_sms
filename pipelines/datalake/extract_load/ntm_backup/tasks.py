# -*- coding: utf-8 -*-
import os
import subprocess
from datetime import timedelta

from google.cloud import storage
from prefect import task as unauthenticated_task

from pipelines.utils.datetime import now
from pipelines.utils.google import dissect_gcs_uri
from pipelines.utils.io import create_tmp_data_folder, get_file_size
from pipelines.utils.logger import log

from .constants import constants as flow_consts


@unauthenticated_task(retries=5, retry_delay_seconds=30)
def dump_task(
  host: str, port: str, database: str, user: str, password: str, filename: str
):

  folder = create_tmp_data_folder()
  out_path = os.path.join(folder, filename)

  log("Iniciando dump...")

  dump_cmd = [
    "mysqldump",
    f"--host={host}",
    f"--port={port}",
    f"--user={user}",
    f"--password={password}",
    "--single-transaction",
    "--routines",
    "--triggers",
    "--events",
    database,
  ]

  with open(out_path, "wb") as f_out:
    dump = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    gzip = subprocess.Popen(
      ["gzip", "-9"], stdin=dump.stdout, stdout=f_out, stderr=subprocess.PIPE
    )
    dump.stdout.close()
    gzip.communicate()
    dump.wait()

  if dump.returncode != 0:
    erro = dump.stderr.read().decode()
    log(f"mysqldump falhou: {erro}", level="error")
    raise RuntimeError(erro)

  tamanho = get_file_size(out_path, pretty=True, raise_if_missing=True)
  log(f"Dump concluído ({tamanho})")
  return out_path


@unauthenticated_task
def upload_daily_to_gcs(bucket_name: str, filepath: str):
  gcs_prefix = flow_consts.GCS_PREFIX.value["daily"]
  filename = os.path.basename(filepath)
  blob_name = f"{gcs_prefix}/{filename}"

  client = storage.Client.from_service_account_json(
    flow_consts.NTM_CREDENTIALS_PATH.value
  )

  log(f"Iniciando upload para gs://{bucket_name}/{blob_name}")
  bucket = client.bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(filepath)
  log(f"Upload concluído: gs://{bucket_name}/{blob_name}")
  return f"gs://{bucket_name}/{blob_name}"


@unauthenticated_task
def copy_daily_gcs_as_weekly(from_uri: str, filename: str):
  uri = dissect_gcs_uri(from_uri)
  bucket_name = uri["bucket"]
  source_blob_name = uri["blob"]

  gcs_prefix = flow_consts.GCS_PREFIX.value["weekly"]
  destination_blob_name = f"{gcs_prefix}/{filename}"

  client = storage.Client.from_service_account_json(
    flow_consts.NTM_CREDENTIALS_PATH.value
  )

  log(
    "Iniciando cópia de "
    f"'gs://{bucket_name}/{source_blob_name}' para "
    f"'gs://{bucket_name}/{destination_blob_name}'"
  )
  bucket = client.bucket(bucket_name)
  source_blob = bucket.blob(source_blob_name)

  bucket.copy_blob(source_blob, bucket, new_name=destination_blob_name)

  log("Arquivo copiado")


@unauthenticated_task
def cleanup_old_gcs_files(bucket_name: str, dry_run: bool = False):
  log("Iniciando limpeza do bucket...")
  limite = now() - timedelta(days=flow_consts.RETENCAO_DIAS.value)
  removidos = 0

  client = storage.Client.from_service_account_json(
    flow_consts.NTM_CREDENTIALS_PATH.value
  )
  bucket = client.bucket(bucket_name)

  for blob in bucket.list_blobs(prefix=flow_consts.GCS_PREFIX.value["daily"]):
    if blob.updated < limite:
      if not dry_run:
        blob.delete()
      removidos += 1
      log(f"Removido do bucket (expirado): {blob.name}")

  log(f"Limpeza concluída: {removidos} arquivo(s) removido(s).")
