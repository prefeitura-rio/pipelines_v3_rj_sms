# -*- coding: utf-8 -*-


def normalize_blob_path(*parts: str) -> str:
	"""
	Monta um caminho de arquivo no padrão do GCS.

	Args:
		*parts: Partes do caminho.

	Returns:
		str: Caminho final com "/" como separador.
	"""
	valid_parts = [str(part).strip("/") for part in parts if part]
	return "/".join(valid_parts)


def build_gdrive_to_gcs_result(
	item: dict, status: str, error_detail: str = None, uploaded_paths: list = None
) -> dict:
	"""
	Monta o resultado do processamento de um arquivo do Drive.

	Args:
		item (dict): Item original do Google Drive.
		status (str): Status do processamento.
		error_detail (str, optional): Detalhe do erro, se houver.
		uploaded_paths (list, optional): Caminhos enviados para o GCS.

	Returns:
		dict: Resultado padronizado do processamento.
	"""
	relative_path = item.get("relative_path") or item.get("name") or ""
	source_folder_name = "/".join(relative_path.split("/")[:-1]) or None

	return {
		"source_file_id": item.get("id"),
		"source_folder_name": source_folder_name,
		"source_file_name": item.get("name"),
		"relative_path": relative_path,
		"status": status,
		"error_detail": error_detail,
		"uploaded_paths": uploaded_paths or [],
	}


def build_execution_summary(items: list[dict]) -> dict:
	"""
	Resume o resultado da execução do stage.

	Args:
		items (list[dict]): Resultados individuais.

	Returns:
		dict: Resumo com totais e lista de itens.
	"""
	return {
		"total_files_found": len(items),
		"total_successful": sum(1 for item in items if item["status"] == "success"),
		"total_failed": sum(1 for item in items if item["status"] == "failed"),
		"items": items,
	}
