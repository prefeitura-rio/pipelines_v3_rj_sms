# -*- coding: utf-8 -*-
import re

from pipelines.utils.logger import log


def extract_nome_gdrive_from_file_name(source_file_name: str) -> str | None:
	if not source_file_name:
		return None

	match = re.search(r"^\d+_\d+_(.+)_\d{8}_", source_file_name)
	return match.group(1) if match else None


def match_gdrive_items_with_depara(
	gdrive_items: list[dict],
	depara_gdrive: list[dict],
) -> list[dict]:
	depara_by_nome = {
		row["nome_gdrive"]: row for row in depara_gdrive if row.get("nome_gdrive")
	}

	matched_items = []
	for item in gdrive_items:
		nome_gdrive = extract_nome_gdrive_from_file_name(item.get("source_file_name"))
		if not nome_gdrive:
			continue

		depara_row = depara_by_nome.get(nome_gdrive)
		if not depara_row:
			continue

		matched_items.append(
			{
				**item,
				"nome_gdrive": nome_gdrive,
				"cnes": str(depara_row["cnes"]),
				"unit_name": depara_row.get("unit_name"),
			}
		)
	log("Match gdrive concluído")
	return matched_items
