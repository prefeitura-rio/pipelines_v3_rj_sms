# -*- coding: utf-8 -*-
import os
import shutil
import sys
from typing import List
import zipfile

from pipelines.utils.cleanup import prettify_byte_size
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


def create_tmp_data_folder(prefix: str = None, suffix: str = None) -> str:
	"""
	Cria uma pasta em `/tmp/data` com nome aleatório para uso geral.
	Opcionalmente recebe um prefixo ou sufixo, adicionados em volta
	dos caracteres aleatórios.

	Retorna o caminho da pasta.
	"""
	if not prefix:
		prefix = "pipelines_"
	if not suffix:
		suffix = ""

	# UUID aqui seria mais "confiável" (e mais lento), mas
	# esses arquivos não vão persistir por muito tempo
	# (cada flow run é um container novo), e `urandom(S)`
	# gera S bytes = S*2 caracteres = 16^(S*2) opções...
	path = f"/tmp/data/{prefix}{os.urandom(8).hex()}{suffix}"
	if os.path.exists(path):
		log(f"Uh oh! '{path}' já existia; deletando...")
		shutil.rmtree(path)
	os.makedirs(path)
	return path


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


def zip_files_from_list(
	filelist: List[str], output_path: str = None, output_filename: str = None
) -> str:
	"""
	Recebe uma lista de caminhos absolutos de arquivos e retorna o caminho
	absoluto de um arquivo ZIP contendo os arquivos passados.
	Opcionalmente recebe o nome do arquivo a ser criado em `output_filename`.
	"""
	if not output_path:
		output_path = create_tmp_data_folder()
	output_path.rstrip("/")

	if not output_filename:
		output_filename = f"{os.urandom(8).hex()}.zip"
	elif not output_filename.endswith(".zip"):
		output_filename = f"{output_filename}.zip"

	zip_filepath = f"{output_path}/{output_filename}"

	log(f"Criando ZIP de {len(filelist)} arquivo(s)...")
	with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
		for file_path in filelist:
			if not os.path.exists(file_path):
				log(f"Arquivo '{file_path}' não existe!", level="error")
				continue
			zipf.write(file_path, arcname=os.path.basename(file_path))

	filesize = os.path.getsize(zip_filepath)
	log(f"Arquivo '{zip_filepath}' tem tamanho {prettify_byte_size(filesize)}")
	return zip_filepath


@task
def zip_files_from_list_task(
	filelist: List[str], output_path: str = None, output_filename: str = None
):
	"""
	Recebe uma lista de caminhos absolutos de arquivos e retorna o caminho
	absoluto de um arquivo ZIP contendo os arquivos passados.
	"""
	return zip_files_from_list(filelist, output_path=output_path)


def unzip_file(filepath: str, output_path: str = None) -> str:
	"""
	Recebe o caminho absoluto de um ZIP, extrai todo o conteúdo dele,
	e retorna o caminho absoluto da pasta contendo os arquivos
	"""
	if not output_path:
		output_path = create_tmp_data_folder()
	output_path.rstrip("/")

	try:
		with zipfile.ZipFile(filepath, "r") as zip_ref:
			log(f"Extraindo conteúdo do ZIP para '{output_path}'")
			zip_ref.extractall(output_path)
	except Exception as e:
		log("Erro extraindo arquivo!", level="error")
		raise e
	return output_path


@task
def unzip_file_task(filepath: str, output_path: str = None):
	return unzip_file(filepath=filepath, output_path=output_path)


def get_file_size(
	path: str,
	raise_if_missing: bool = False,
	raise_if_not_file: bool = False,
	pretty: bool = False,
) -> str | int:
	"""
	Retorna tamanho do arquivo especificado. Por padrão, caso o arquivo
	não exista ou o caminho não seja um arquivo, retorna 0; opcionalmente
	dispara `RuntimeError`s nesses casos.

	Se `pretty=True`, retorna string já formatada (ex.: '4.8 MiB').
	"""
	if not os.path.exists(path):
		if raise_if_missing:
			raise RuntimeError(f"Arquivo '{path}' não existe!")
		return 0
	if not os.path.isfile(path):
		if raise_if_not_file:
			raise RuntimeError(f"'{path}' não é um arquivo!")
		return 0
	size = os.path.getsize(path)
	if pretty:
		return prettify_byte_size(size)
	return size
