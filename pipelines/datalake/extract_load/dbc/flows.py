# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.google import (
  dissect_gcs_uri,
  download_file_from_bucket_task,
  upload_to_cloud_storage_task,
)
from pipelines.utils.prefect import flow, flow_config

from .tasks import run_conversion


@flow(
  name="Extração: DBC e DBF",
  description="Converte arquivos DBC e DBF para CSV a partir de um URI de bucket GCS",
  owners=[CIT.AVELLAR_ID.value],
  tags=["CIT"],
)
def extract_dbc(gcs_uri: str, environment: str = "dev"):
  # Primeiro, baixamos o arquivo DBC/DBF para o disco local
  dbc_filepath = download_file_from_bucket_task(gcs_uri=gcs_uri)
  # Em seguida, fazemos a descompressão/conversão
  csv_filepath = run_conversion(filepath=dbc_filepath)
  # Por fim, upload dos resultados para o bucket novamente
  uri = dissect_gcs_uri(gcs_uri)
  upload_to_cloud_storage_task(
    path=csv_filepath, bucket_name=uri["bucket"], blob_prefix=uri["blob"]
  )


_flows = [flow_config(flow=extract_dbc)]
