# -*- coding: utf-8 -*-
from prefect.futures import wait

from pipelines.constants import constants as global_consts
from pipelines.utils.google import (
	dissect_gcs_uri,
	download_file_from_bucket_task,
	upload_to_cloud_storage_task,
)
from pipelines.utils.io import (
	list_files_in_folder_task,
	unzip_file_task,
	zip_files_from_list_task,
)
from pipelines.utils.prefect import flow
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import run_conversion, upload_csv_as_table


@flow(
	name="DataLake - Extração e Carga de Dados - GDB",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.AVELLAR_ID.value],
	description="Converte arquivos GDB para CSV a partir de um URI de bucket GCS",
)
def extract_gdb(
	# URI do GCS do arquivo a ser convertido
	# Formato 'gs://bucket/caminho/do/arquivo.gdb'
	gcs_uri: str,
	# Dataset em que as tabelas serão colocadas
	dataset: str = "brutos_gdb_{cnes/sih/sia}",
	# Máximo de linhas de CSV a fazer upload de uma vez
	lines_per_chunk: int = 100_000,
	# Mês referência do dado, usado pra data_particao;
	# p.ex.: "2025-08" para backup do CNES de agosto/2025
	# O flow consegue inferir a data referência se o nome do arquivo termina com ela
	# p.ex.: 'CNES022024.GDB' -> '2024-02'
	data_referencia: str = None,
	# ...
	environment: str = "dev",
):
	# Baixa o arquivo do bucket
	downloaded_file = download_file_from_bucket_task(gcs_uri=gcs_uri)

	# Podemos receber diretamente um ZIP já extraído, somente para
	# upload de CSVs como tabelas; se esse for o caso, pula a parte
	# de extração em si
	if not gcs_uri.endswith(".zip"):
		# Extrai tabelas do GDB, escreve como CSVs
		csv_folder = run_conversion(filepath=downloaded_file)
		csv_files = list_files_in_folder_task(folder=csv_folder, endswith=".csv")

		# Coloca os CSVs em um ZIP, faz upload de volta para o GCS
		uri = dissect_gcs_uri(gcs_uri)
		zip_path = zip_files_from_list_task(filelist=csv_files)
		upload_to_cloud_storage_task(
			path=zip_path, bucket_name=uri["bucket"], blob_prefix=uri["blob"]
		)
	# Se recebemos um ZIP...
	else:
		csv_folder = unzip_file_task(filepath=downloaded_file)
		csv_files = list_files_in_folder_task(folder=csv_folder, endswith=".csv")

	# Também faz upload de cada um deles como tabelas no BigQuery
	bq_futures = [
		upload_csv_as_table.submit(filepath, gcs_uri, data_referencia, lines_per_chunk)
		for filepath in csv_files
	]
	# Espera todos os uploads terminarem
	wait(bq_futures)


_flows = [extract_gdb]
_schedules = []
