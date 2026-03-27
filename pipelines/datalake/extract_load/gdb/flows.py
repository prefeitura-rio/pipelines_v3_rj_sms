# -*- coding: utf-8 -*-
from prefect.futures import wait

from pipelines.constants import constants as global_consts
from pipelines.utils.google import (
	dissect_gcs_uri,
	download_file_from_bucket_task,
	upload_to_cloud_storage_task,
)
from pipelines.utils.io import list_files_in_folder_task
from pipelines.utils.prefect import flow
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import run_conversion


@flow(
	name="DataLake - Extração e Carga de Dados - GDB",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.AVELLAR_ID.value],
	description="Converte arquivos GDB para CSV a partir de um URI de bucket GCS",
)
def extract_dbc(file_gs_uri: str, environment: str = "dev"):
	# Baixa GDB do bucket
	gdb_filepath = download_file_from_bucket_task(gcs_uri=file_gs_uri)
	# Extrai tabelas do GDB, escreve como CSVs
	csv_folder = run_conversion(filepath=gdb_filepath)
	# Lista todos os CSVs exportados
	file_list = list_files_in_folder_task(folder=csv_folder, endswith=".csv")
	# Faz upload de cada um deles, de forma paralela
	uri = dissect_gcs_uri(file_gs_uri)
	futures = [
		upload_to_cloud_storage_task.submit(filepath, uri["bucket"], uri["blob"])
		for filepath in file_list
	]
	# Espera todos os uploads terminarem
	wait(futures)


_flows = [extract_dbc]
_schedules = []
