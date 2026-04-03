# -*- coding: utf-8 -*-
import posixpath


def normalize_blob_path(*parts: str) -> str:
	valid_parts = [str(part).strip("/") for part in parts if part]
	if not valid_parts:
		return ""
	return posixpath.join(*valid_parts)


def build_result(
	item: dict, status: str, error_detail: str = None, uploaded_paths: list = None
) -> dict:
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


def build_execution_summary(items: list[dict]) -> dict:
	return {
		"total_files_found": len(items),
		"total_successful": sum(1 for item in items if item.get("status") == "success"),
		"total_failed": sum(1 for item in items if item.get("status") == "failed"),
		"items": items,
	}
