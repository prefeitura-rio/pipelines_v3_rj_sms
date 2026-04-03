# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.google import (
	get_fully_qualified_bucket_name,
	list_google_drive_files,
)
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import process_google_drive_file
from .utils import build_execution_summary


@flow(
	name="Migrate - Google Drive to GCS",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.CIT_ID.value],
	description="Lista arquivos do Google Drive e faz upload para o GCS",
)
def gdrive_to_gcs(
	root_folder_id: str,
	bucket_name: str,
	environment: str = "dev",
	last_modified_date: str = None,
	blob_prefix: str = None,
):
	resolved_bucket_name = get_fully_qualified_bucket_name(
		bucket_name=bucket_name,
		environment=environment,
	)
	log(f"Nome do Bucket final: '{resolved_bucket_name}'")

	items = list_google_drive_files(
		folder_id=root_folder_id,
		last_modified_date=last_modified_date,
	)
	log(f"Encontrado(s) {len(items)} arquivo(s) no Google Drive")

	results = []
	for item in items:
		results.append(
			process_google_drive_file(
				item=item,
				bucket_name=resolved_bucket_name,
				blob_prefix=blob_prefix,
			)
		)

	return build_execution_summary(results)


_flows = [flow_config(flow=gdrive_to_gcs)]
