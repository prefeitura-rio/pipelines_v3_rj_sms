# -*- coding: utf-8 -*-
from pipelines.constants import constants
from pipelines.utils.datalake import upload_to_datalake
from pipelines.utils.prefect import flow, rename_flow_run
from pipelines.utils.infisical import inject_bd_credentials
from pipelines.utils.io import create_data_folders, download_google_sheets
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules


@flow(
	name="DataLake - Extração e Carga de Dados - Google Sheets",
	state_handlers=[handle_flow_state_change],
	owners=[constants.CIT_ID.value],
)
def sms_dump_url(
	# URL da planilha
	url: str,
	# Nome da aba na planilha; por padrão "Sheet1"
	gsheets_sheet_name: str,
	# Dataset no BigQuery
	dataset_id: str,
	# Tabela no BigQuery
	table_id: str,
	# Renomear flow para nome legível
	rename_flow: bool = False,
	# url_type: str = "google_sheet"  (removido por ser redundante)
	# csv_delimiter: str = ";"        (removido por ser redundante)
	environment: str = "dev",
):
	#####################################
	# Configura ambiente
	####################################
	credentials = inject_bd_credentials(environment=environment)

	if rename_flow:
		rename_flow_run(f"Dump URL: {dataset_id}.{table_id}")

	####################################
	# Parte 1 - Obter o dado
	#####################################
	create_folders_task = create_data_folders()

	download_task = download_google_sheets(
		url=url,
		file_path=create_folders_task["raw"],
		file_name=table_id,
		gsheets_sheet_name=gsheets_sheet_name,
		wait_for=[credentials, create_folders_task],
	)

	#####################################
	# Parte 2 - Upload do dado
	#####################################
	upload_to_datalake(
		input_path=create_folders_task["raw"],
		dataset_id=dataset_id,
		table_id=table_id,
		if_exists="replace",
		dump_mode="overwrite",
		delete_mode="staging",
		if_storage_data_exists="replace",
		biglake_table=True,
		dataset_is_public=False,
		wait_for=[download_task],
	)


_flows = [sms_dump_url]
_schedules = schedules
