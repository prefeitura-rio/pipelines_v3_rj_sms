# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.migrate.gdrive_to_gcs.flows import gdrive_to_gcs
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import (
	get_depara_gdrive,
	get_vitacare_unit_info,
	populate_pipeline_status_table,
	update_pipeline_status_from_gdrive_to_gcs,
)
from .utils import match_gdrive_items_with_depara


@flow(
	name="DataLake - Extração e Carga de Dados - Vitacare Historico",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.DANIEL_ID.value],
	description="Orquestra a preparação da tabela de controle e a etapa gdrive_to_gcs",
)
def vitacare_historico(
	root_folder_id: str,
	bucket_name: str,
	environment: str = "dev",
	last_modified_date: str = None,
	blob_prefix: str = None,
):
	reference_month = from_relative_date("M-0")
	
	expected_units = get_vitacare_unit_info()
	
	populate_pipeline_status_table(
		expected_units=expected_units,
		reference_month=reference_month,
	)
	
	depara_gdrive = get_depara_gdrive()
	
	gdrive_to_gcs_result = gdrive_to_gcs(
		root_folder_id=root_folder_id,
		bucket_name=bucket_name,
		environment=environment,
		last_modified_date=last_modified_date,
		blob_prefix=blob_prefix,
	)
	matched_items = match_gdrive_items_with_depara(
		gdrive_items=gdrive_to_gcs_result["items"],
		depara_gdrive=depara_gdrive,
	)
	update_pipeline_status_from_gdrive_to_gcs(
		reference_month=reference_month,
		matched_items=matched_items,
	)

	return {
		"reference_month": reference_month,
		"expected_units": expected_units,
		"depara_gdrive": depara_gdrive,
		"gdrive_to_gcs_result": gdrive_to_gcs_result,
		"matched_items": matched_items,
	}


_flows = [flow_config(flow=vitacare_historico)]
