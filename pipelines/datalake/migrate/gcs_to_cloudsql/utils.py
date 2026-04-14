# -*- coding: utf-8 -*-


def validate_restore_input(item: dict) -> None:
	"""
	Valida os campos mínimos necessários para restauração no Cloud SQL.

	Args:
		item (dict): Item de entrada.

	Returns:
		None
	"""
	if not item.get("source_uri"):
		raise ValueError("Item sem 'source_uri'.")

	if not item.get("database_name"):
		raise ValueError("Item sem 'database_name'.")


def build_gcs_to_cloudsql_result(
	item: dict, status: str, error_detail: str = None
) -> dict:
	"""
	Monta o resultado padronizado do processamento de um item.

	Args:
		item (dict): Item original da etapa.
		status (str): Status do processamento.
		error_detail (str, optional): Detalhe do erro, se houver.

	Returns:
		dict: Resultado padronizado do processamento.
	"""
	return {
		"source_uri": item.get("source_uri"),
		"database_name": item.get("database_name"),
		"status": status,
		"error_detail": error_detail,
		"metadata": item.get("metadata") or {},
	}
