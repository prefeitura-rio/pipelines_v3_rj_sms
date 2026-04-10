# -*- coding: utf-8 -*-
import os
import posixpath
import shutil
import zipfile

from pipelines.utils.google import download_google_drive_file, upload_to_cloud_storage
from pipelines.utils.io import create_tmp_data_folder, list_files_in_folder, unzip_file
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .utils import build_gdrive_to_gcs_result, normalize_blob_path


@task
def process_google_drive_file(item: dict, bucket_name: str) -> dict:
	"""
	Processa um arquivo do Google Drive e envia seu conteúdo para o GCS.

	Args:
		item (dict): Metadados do arquivo retornados pela listagem do Drive.
		bucket_name (str): Nome do bucket de destino no GCS.

	Returns:
		dict: Resultado do processamento do arquivo.
	"""
	relative_path = item.get("relative_path") or item.get("name")
	if not relative_path:
		return build_gdrive_to_gcs_result(
			item=item, status="failed", error_detail="Item sem 'relative_path' ou 'name'."
		)

	if not item.get("id"):
		return build_gdrive_to_gcs_result(
			item=item, status="failed", error_detail="Item sem 'id'."
		)

	tmp_root = create_tmp_data_folder(prefix="gdrive_to_gcs_")
	uploaded_paths = []

	try:
		# Baixa o arquivo para a área temporária local
		download_path = os.path.join(tmp_root, relative_path)
		log(f"Baixando arquivo do Google Drive para '{download_path}'")
		downloaded_file = download_google_drive_file(
			file_id=item["id"], destination_path=download_path
		)

		# Se for ZIP, extrai e envia os arquivos sem incluir o nome do ZIP no caminho final
		if downloaded_file.lower().endswith(".zip"):
			extracted_dir = os.path.join(tmp_root, "extracted")
			unzip_file(filepath=downloaded_file, output_path=extracted_dir)

			files_to_upload = list_files_in_folder(extracted_dir, recursive=True)
			source_parent = posixpath.dirname(relative_path)

			for file_path in files_to_upload:
				extracted_relative_path = os.path.relpath(
					file_path, extracted_dir
				).replace("\\", "/")
				blob_path = normalize_blob_path(source_parent, extracted_relative_path)
				blob_dir = posixpath.dirname(blob_path) or None

				upload_to_cloud_storage(
					path=file_path, bucket_name=bucket_name, blob_prefix=blob_dir
				)
				uploaded_paths.append(f"gs://{bucket_name}/{blob_path}")

		else:
			blob_path = normalize_blob_path(relative_path)
			blob_dir = posixpath.dirname(blob_path) or None

			upload_to_cloud_storage(
				path=downloaded_file, bucket_name=bucket_name, blob_prefix=blob_dir
			)
			uploaded_paths.append(f"gs://{bucket_name}/{blob_path}")

		return build_gdrive_to_gcs_result(
			item=item, status="success", uploaded_paths=uploaded_paths
		)

	except zipfile.BadZipFile as exc:
		log(f"Arquivo ZIP corrompido em '{relative_path}': {repr(exc)}", level="error")
		return build_gdrive_to_gcs_result(
			item=item,
			status="failed",
			error_detail=f"Arquivo ZIP corrompido: {exc}",
			uploaded_paths=uploaded_paths,
		)

	except Exception as exc:  # pylint: disable=broad-except
		log(f"Erro processando arquivo '{relative_path}': {repr(exc)}", level="error")
		return build_gdrive_to_gcs_result(
			item=item,
			status="failed",
			error_detail=str(exc),
			uploaded_paths=uploaded_paths,
		)

	finally:
		# Limpa os arquivos temporários ao final do processamento
		if os.path.exists(tmp_root):
			log(f"Apagando arquivos temporários em '{tmp_root}'")
			shutil.rmtree(tmp_root, ignore_errors=True)
