# -*- coding: utf-8 -*-
from datetime import date

from azure.storage.blob import BlobServiceClient

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


def download_azure_blob(
  container_name: str,
  blob_path: str,
  file_folder: str,
  file_name: str,
  credentials: str,
  add_load_date_to_filename: bool = False,
  load_date: str = None,
) -> str:
  """
  Baixa um arquivo do Azure Blob Storage para uma pasta local.
  """
  log(f"Baixando arquivo do Azure Blob Storage: {blob_path}")
  blob_service_client = BlobServiceClient(
    account_url="https://datalaketpcgen2.blob.core.windows.net/", credential=credentials
  )
  blob_client = blob_service_client.get_blob_client(
    container=container_name, blob=blob_path
  )

  if add_load_date_to_filename:
    filename_date = load_date or str(date.today())
    destination_file_path = f"{file_folder}/{file_name}_{filename_date}.csv"
  else:
    destination_file_path = f"{file_folder}/{file_name}.csv"

  with open(destination_file_path, "wb") as file:
    blob_data = blob_client.download_blob()
    blob_data.readinto(file)

  log(f"Blob baixado para '{destination_file_path}'")
  return destination_file_path


@task(retries=3, retry_delay_seconds=30)
def download_azure_blob_task(
  container_name: str,
  blob_path: str,
  file_folder: str,
  file_name: str,
  credentials: str,
  add_load_date_to_filename: bool = False,
  load_date: str = None,
) -> str:
  """
  Task Prefect v3 para baixar arquivo do Azure Blob Storage.
  """
  return download_azure_blob(
    container_name=container_name,
    blob_path=blob_path,
    file_folder=file_folder,
    file_name=file_name,
    credentials=credentials,
    add_load_date_to_filename=add_load_date_to_filename,
    load_date=load_date,
  )
