# -*- coding: utf-8 -*-

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .utils import (
	validate_restore_input,
	validate_database_name,
	build_gcs_to_cloudsql_result,
	delete_database,
	import_backup_to_database,
	database_exists,
)


@task
def restore_gcs_backup_to_cloudsql(item: dict, instance_name: str) -> dict:
	"""
	Realiza o restore de um backup (.bak) para o Cloud SQL.

	Args:
		item (dict): Item contendo source_uri, database_name e metadata.
		instance_name (str): Nome da instância do Cloud SQL.

	Returns:
		dict: Resultado do processamento do item.
	"""
	source_uri = item.get("source_uri")
	database_name = item.get("database_name")

	try:
		log(f"(restore_gcs_backup_to_cloudsql) iniciando restore de '{database_name}'")

		# Valida entrada mínima
		validate_restore_input(item)
		validate_database_name(database_name)

		# Remove database anterior, se existir
		if database_exists(instance_name, database_name):
			log(
				f"(restore_gcs_backup_to_cloudsql) database '{database_name}' já existe, deletando..."
			)
			delete_database(
				instance_name=instance_name,
				database_name=database_name,
			)
		else:
			log(
				f"(restore_gcs_backup_to_cloudsql) database '{database_name}' não existe, seguindo..."
			)

		# Importa backup
		import_backup_to_database(
			instance_name=instance_name,
			source_uri=source_uri,
			database_name=database_name,
		)

		# Validação final: database foi criada
		if not database_exists(instance_name, database_name):
			raise Exception(f"Database '{database_name}' não encontrada após import.")

		log(f"(restore_gcs_backup_to_cloudsql) restore concluído com sucesso")

		return build_gcs_to_cloudsql_result(
			item=item,
			status="success",
		)

	except Exception as exc:
		log(
			f"(restore_gcs_backup_to_cloudsql) erro no restore: {repr(exc)}",
			level="error",
		)

		return build_gcs_to_cloudsql_result(
			item=item,
			status="failed",
			error_detail=str(exc),
		)
