# -*- coding: utf-8 -*-
import os
import posixpath
import shutil
import zipfile

from pipelines.utils.google import download_google_drive_file, upload_to_cloud_storage
from pipelines.utils.io import create_tmp_data_folder, list_files_in_folder, unzip_file
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


def _normalize_blob_path(*parts: str) -> str:
	valid_parts = [str(part).strip("/") for part in parts if part]
	if not valid_parts:
		return ""
	return posixpath.join(*valid_parts)


def _build_result(item: dict, status: str, error_detail: str = None, uploaded_paths: list = None):
	relative_path = item.get("relative_path") or item.get("name") or ""
	relative_dir = posixpath.dirname(relative_path)
	source_folder_name = item.get("source_folder_name")
	if source_folder_name is None:
		source_folder_name = relative_dir or None

	return {
		"ap": item.get("ap"),
		"unit_name": item.get("unit_name"),
		"source_folder_name": source_folder_name,
		"source_file_name": item.get("name"),
		"relative_path": relative_path,
		"status": status,
		"error_detail": error_detail,
		"uploaded_paths": uploaded_paths or [],
	}


@task
def process_google_drive_file(
	item: dict,
	bucket_name: str,
	blob_prefix: str = None,
) -> dict:
	relative_path = item.get("relative_path") or item.get("name")
	if not relative_path:
		return _build_result(
			item=item,
			status="failed",
			error_detail="Item sem 'relative_path' ou 'name'.",
		)

	if not item.get("id"):
		return _build_result(
			item=item,
			status="failed",
			error_detail="Item sem 'id'.",
		)

	tmp_root = create_tmp_data_folder(prefix="gdrive_to_gcs_")
	uploaded_paths = []

	try:
		download_path = os.path.join(tmp_root, relative_path)
		log(f"Baixando arquivo do Google Drive para '{download_path}'")
		downloaded_file = download_google_drive_file(
			file_id=item["id"],
			destination_path=download_path,
		)

		if downloaded_file.lower().endswith(".zip"):
			extracted_dir = os.path.join(tmp_root, "extracted")
			unzip_file(filepath=downloaded_file, output_path=extracted_dir)

			files_to_upload = list_files_in_folder(extracted_dir, recursive=True)
			source_parent = posixpath.dirname(relative_path)

			for file_path in files_to_upload:
				extracted_relative_path = os.path.relpath(file_path, extracted_dir)
				blob_path = _normalize_blob_path(
					blob_prefix,
					source_parent,
					extracted_relative_path,
				)
				blob_dir = posixpath.dirname(blob_path) or None

				upload_to_cloud_storage(
					path=file_path,
					bucket_name=bucket_name,
					blob_prefix=blob_dir,
				)
				uploaded_paths.append(blob_path)
		else:
			blob_path = _normalize_blob_path(blob_prefix, relative_path)
			blob_dir = posixpath.dirname(blob_path) or None

			upload_to_cloud_storage(
				path=downloaded_file,
				bucket_name=bucket_name,
				blob_prefix=blob_dir,
			)
			uploaded_paths.append(blob_path)

		return _build_result(
			item=item,
			status="success",
			uploaded_paths=uploaded_paths,
		)
	except zipfile.BadZipFile as exc:
		log(f"Arquivo ZIP corrompido em '{relative_path}': {repr(exc)}", level="error")
		return _build_result(
			item=item,
			status="failed",
			error_detail=f"Arquivo ZIP corrompido: {exc}",
			uploaded_paths=uploaded_paths,
		)
	except Exception as exc:  # pylint: disable=broad-except
		log(f"Erro processando arquivo '{relative_path}': {repr(exc)}", level="error")
		return _build_result(
			item=item,
			status="failed",
			error_detail=str(exc),
			uploaded_paths=uploaded_paths,
		)
	finally:
		if os.path.exists(tmp_root):
			log(f"Apagando arquivos temporários em '{tmp_root}'")
			shutil.rmtree(tmp_root, ignore_errors=True)
