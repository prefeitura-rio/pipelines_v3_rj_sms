# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import restore_gcs_backup_to_cloudsql
from .utils import (
	ensure_instance_running,
	ensure_instance_stopped,
	build_gcs_to_cloudsql_summary,
)


@flow(
	name="Migrate - GCS to Cloud SQL",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.CIT_ID.value],
	description="Restaura backups (.bak) do GCS para o Cloud SQL",
)
def gcs_to_cloudsql(
	items: list[dict],
	instance_name: str,
):
	"""
	Processa uma lista de backups e restaura no Cloud SQL.

	Args:
		items (list[dict]): Lista de itens com source_uri e database_name.
		instance_name (str): Nome da instância do Cloud SQL.

	Returns:
		dict: Resumo da execução.
	"""
	if not items:
		log("(gcs_to_cloudsql) nenhum item recebido para processamento")
		return build_gcs_to_cloudsql_summary([])

	log(f"(gcs_to_cloudsql) iniciando processamento de {len(items)} item(s)")

	results = []

	try:
		# Liga instância antes de começar
		ensure_instance_running(instance_name)

		# Processa sequencialmente cada item
		for item in items:
			results.append(
				restore_gcs_backup_to_cloudsql(
					item=item,
					instance_name=instance_name,
				)
			)

	finally:
		# Garante que a instância será desligada mesmo em caso de erro
		ensure_instance_stopped(instance_name)

	log("(gcs_to_cloudsql) processamento finalizado")

	return build_gcs_to_cloudsql_summary(results)


_flows = [flow_config(flow=gcs_to_cloudsql)]