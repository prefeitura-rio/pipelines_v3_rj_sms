# -*- coding: utf-8 -*-
from pipelines.utils.logger import log


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
  item: dict,
  status: str,
  error_detail: str = None,
  uploaded_paths: list = None,
  inner_file_paths: list = None,
) -> dict:
  """
  Monta o resultado do processamento de um arquivo do Drive.

  Args:
          item (dict): Item original do Google Drive.
          status (str): Status do processamento.
          error_detail (str, optional): Detalhe do erro, se houver.
          uploaded_paths (list, optional): Caminhos enviados para o GCS.
          inner_file_paths (list, optional): Caminhos internos encontrados em arquivos ZIP.

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
    "inner_file_paths": inner_file_paths or [],
  }


def build_execution_summary(items: list[dict]) -> dict:
  """
  Resume o resultado da execução do stage.

  Args:
          items (list[dict]): Resultados individuais.

  Returns:
          dict: Resumo com totais e lista de itens.
  """
  failed_items = [
    {
      "source_file_id": item.get("source_file_id"),
      "source_file_name": item.get("source_file_name"),
      "relative_path": item.get("relative_path"),
      "error_detail": item.get("error_detail"),
    }
    for item in items
    if item.get("status") != "success"
  ]

  summary = {
    "total_files_found": len(items),
    "total_successful": sum(1 for item in items if item["status"] == "success"),
    "total_failed": len(failed_items),
    "failed_items": failed_items,
    "items": items,
  }

  if failed_items:
    failed_items_log = "\n".join(
      (
        "- "
        f"{item.get('relative_path') or item.get('source_file_name') or '<sem nome>'} "
        f"(id={item.get('source_file_id') or '<sem id>'}) "
        f"erro={item.get('error_detail') or '<sem detalhe>'}"
      )
      for item in failed_items
    )
    log(
      "Arquivos sem sucesso no gdrive_to_gcs:\n"
      f"Total: {summary['total_failed']}/{summary['total_files_found']}\n"
      f"{failed_items_log}",
      level="warning",
    )
  else:
    log(
      "Todos os arquivos do gdrive_to_gcs foram processados com sucesso "
      f"({summary['total_successful']}/{summary['total_files_found']})"
    )

  return summary
