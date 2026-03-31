# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.google import (
	dissect_gcs_uri,
	download_file_from_bucket_task,
	upload_to_cloud_storage_task,
)
from pipelines.utils.prefect import flow
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import run_conversion


@flow(
	name="DataLake - Extração e Carga de Dados - DBC e DBF",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.AVELLAR_ID.value],
	description="Converte arquivos DBC e DBF para CSV a partir de um URI de bucket GCS",
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


_flows = [{"flow": extract_dbc}]
