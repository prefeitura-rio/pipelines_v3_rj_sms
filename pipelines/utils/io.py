# -*- coding: utf-8 -*-
import os
import shutil
import sys

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def create_data_folders() -> dict[str, str]:
	"""
	Cria duas pastas, './data/raw' e './data/partition_directory',
	e retorna seus caminhos completos. Caso uma pasta já exista,
	apaga seu conteúdo.

	Returns:
		dict: O dicionário:
		{
			"data": "{cwd}/data/raw"
			"partition_directory": "{cwd}/data/partition_directory"
		}
	"""
	try:
		path_raw = os.path.join(os.getcwd(), "data", "raw")
		path_partition = os.path.join(os.getcwd(), "data", "partition_directory")

		if os.path.exists(path_raw):
			shutil.rmtree(path_raw, ignore_errors=False)
		os.makedirs(path_raw)

		if os.path.exists(path_partition):
			shutil.rmtree(path_partition, ignore_errors=False)
		os.makedirs(path_partition)

		folders = {"raw": path_raw, "partition_directory": path_partition}

		log(f"Pastas criadas: {folders}")
		return folders

	except Exception as e:  # pylint: disable=W0703
		sys.exit(f"Problema ao criar pastas: {e}")
