# -*- coding: utf-8 -*-
import os
import shutil
import sys

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def create_data_folders_task() -> dict[str, str]:
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


def list_files_in_folder(folder: str, endswith: str = None, recursive: bool = False):
	"""
	Retorna uma lista com o caminho de arquivos em uma pasta.
	Args:
		folder(str):
			Caminho da pasta
		endswith(str?):
			Filtro pelo final de arquivos; ex. `endswith=".csv"`
		recursive(bool?):
			Quando `recursive=True` é passado, também procura por
			arquivos em subpastas
	"""
	files = []
	for dirpath, _, filenames in os.walk(folder):
		files.extend(
			[
				os.path.join(dirpath, file)
				for file in filenames
				if (not endswith or file.endswith(endswith))
			]
		)
		if not recursive:
			break
	return files


@task
def list_files_in_folder_task(folder: str, endswith: str = None, recursive: bool = False):
	"""
	Retorna uma lista com o caminho de arquivos em uma pasta.
	Args:
		folder(str):
			Caminho da pasta
		endswith(str?):
			Filtro pelo final de arquivos; ex. `endswith=".csv"`
		recursive(bool?):
			Quando `recursive=True` é passado, também procura por
			arquivos em subpastas
	"""
	return list_files_in_folder(folder, endswith=endswith, recursive=recursive)
