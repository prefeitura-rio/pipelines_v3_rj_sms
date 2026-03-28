# -*- coding: utf-8 -*-
import csv
import os
from typing import Iterator, List, Literal

import gspread
import pandas as pd

from google.cloud import storage
from google.cloud.storage.blob import Blob

from pipelines.utils.cleanup import prettify_byte_size, cleanup_columns_for_bigquery
from pipelines.utils.infisical import get_credentials_from_env
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task()
def download_google_sheets_task(
	url: str,
	file_path: str,
	file_name: str,
	gsheets_sheet_name: str,
	csv_delimiter: str = ";",
) -> None:
	"""
	Baixa uma planilha Google Sheets, a partir de seu URL, e salva como
	um arquivo CSV local.

	Args:
		url(str): URL da planilha a ser baixada
		file_path(str): Caminho de destino do arquivo
		file_name(str): Nome do arquivo local que será criado
		gsheets_sheet_name(str): Nome da planilha (aba) a ser baixada
		csv_delimiter(str?): Delimitador a ser usado no CSV, ";" por padrão
	"""
	if not file_name.endswith(".csv"):
		file_name = file_name + ".csv"
	filepath = os.path.join(file_path, file_name)

	if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
		raise ValueError(
			"Variável de ambiente `GOOGLE_APPLICATION_CREDENTIALS` não está configurada "
			"e é necessária para baixar Google Sheets."
		)

	credentials = get_credentials_from_env(
		scopes=[
			"https://www.googleapis.com/auth/spreadsheets",
			"https://www.googleapis.com/auth/drive",
		]
	)

	url_prefix = "https://docs.google.com/spreadsheets/d/"
	if not url.startswith(url_prefix):
		raise ValueError(f"URL inválida: '{url}'! Precisa ser do tipo '{url_prefix}...'")

	gspread_client = gspread.authorize(credentials)
	# Cria dataframe a partir da planilha
	dataframe = pd.DataFrame(
		gspread_client.open_by_url(url).worksheet(gsheets_sheet_name).get_values()
	)  # fmt=skip
	# Primeira linha contém cabeçalho
	new_header = dataframe.iloc[0]
	# Remove cabeçalho dos dados
	dataframe = dataframe[1:]
	# Redefine colunas como cabeçalho obtido anteriormente
	dataframe.columns = new_header

	log(f">>>>> Dataframe shape: {dataframe.shape}")
	log(f">>>>> Dataframe colunas (cruas):    {dataframe.columns}")
	dataframe.columns = cleanup_columns_for_bigquery(dataframe, raise_on_repeats="raise")
	log(f">>>>> Dataframe colunas (tratadas): {dataframe.columns}")

	dataframe.to_csv(
		filepath, index=False, sep=csv_delimiter, encoding="utf-8", quoting=csv.QUOTE_ALL
	)


def download_path_from_bucket(
	path: str, bucket_name: str, blob_prefix: str = None
) -> List[str]:
	"""
	Baixa arquivos do Google Cloud Storage para um caminho especificado

	Args:
		path (str): Caminho local para onde baixar os arquivos
		bucket_name (str): Nome do bucket do Google Cloud Storage
		blob_prefix (str?): Prefixo dos blobs para baixar. Por padrão, é `None`

	Returns:
		out (list[str]): Lista com caminho local de cada arquivo baixado
	"""
	client = storage.Client()
	bucket = client.get_bucket(bucket_name)
	blobs: Iterator[Blob] = bucket.list_blobs(prefix=blob_prefix)

	if not os.path.exists(path):
		os.makedirs(path)

	downloaded_files = []
	for blob in blobs:
		destination_file_name: str = os.path.join(path, blob.name)
		os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)
		try:
			blob.download_to_filename(destination_file_name)
			downloaded_files.append(destination_file_name)
		except IsADirectoryError:
			pass

	log(f"Baixado(s) {len(downloaded_files)} arquivo(s) do bucket '{bucket_name}'")
	return downloaded_files


def dissect_gcs_uri(uri: str):
	if uri is None:
		raise ValueError("URI nula!")

	uri = str(uri).removeprefix("gs://")
	if len(uri) <= 0:
		raise ValueError("URI vazia!")

	# Separa caminho e nome do arquivo
	# ex.: 'bucket_name/path/to/file/BACKUP.GDB'
	#      => [ 'bucket_name/path/to/file', 'BACKUP.GDB' ]
	(gcs_full_path, filename) = uri.rsplit("/", maxsplit=1)

	# Separa bucket e caminho ("blob")
	if "/" in gcs_full_path:
		# 'bucket_name/path/to/file'
		# => [ 'bucket_name', 'path/to/file' ]
		(bucket_name, blob_name) = gcs_full_path.split("/", maxsplit=1)
	else:
		# Arquivo na raiz do bucket
		(bucket_name, blob_name) = (gcs_full_path, "")

	# 'VERY.IMPORTANT.BACKUP.GDB'
	# => [ 'VERY.IMPORTANT.BACKUP', 'GDB' ]
	if "." in filename:
		(raw_filename, suffix) = filename.rsplit(".", maxsplit=1)
	else:
		(raw_filename, suffix) = (filename, "")

	return {
		"bucket": bucket_name,
		"blob": blob_name,
		"full_path": f"{blob_name}/{filename}" if blob_name else filename,
		"filename": filename,
		"filename_no_ext": raw_filename,
		"file_ext": suffix,
	}


def download_file_from_bucket(gcs_uri: str, to_dir: str = None):
	"""
	Baixa um único arquivo do Google Cloud Storage a partir de um
	URI 'gs://...' para um arquivo local, e retorna seu caminho.
	Se `to_dir` não for passado, uma pasta em /tmp/data será criada
	"""
	uri = dissect_gcs_uri(gcs_uri)

	client = storage.Client()
	bucket = client.get_bucket(uri["bucket"])
	blob = bucket.blob(uri["full_path"])

	if not to_dir:
		to_dir = create_tmp_data_folder()

	os.makedirs(to_dir, exist_ok=True)
	full_file_path = f"{to_dir}/{uri['filename']}"
	if os.path.exists(full_file_path):
		log(f"Arquivo já existe em '{full_file_path}'! Sobrescrevendo...")
	with open(full_file_path, "w") as f:
		file_path = f.name
		log(f"Baixando '{gcs_uri}' para '{file_path}'")
		blob.download_to_filename(file_path)

	filesize = os.path.getsize(full_file_path)
	log(f"Arquivo '{full_file_path}' tem tamanho {prettify_byte_size(filesize)}")
	return full_file_path


@task
def download_file_from_bucket_task(gcs_uri: str):
	"""
	Baixa um único arquivo do Google Cloud Storage a partir de um
	URI 'gs://...' para um arquivo local, e retorna seu caminho
	"""
	return download_file_from_bucket(gcs_uri)


def upload_to_cloud_storage(
	path: str,
	bucket_name: str,
	blob_prefix: str = None,
	if_exists: Literal["raise", "replace", "pass"] = "replace",
) -> None:
	"""
	Faz upload de arquivo ou pasta para o Google Cloud Storage

	Args:
		path (str):
			Caminho do arquivo ou pasta a ser enviado.
		bucket_name (str):
			Nome do bucket no Google Cloud Storage.
		blob_prefix (str?):
			Caminho no bucket para o arquivo. Por padrão, é `None`.
		if_exists (str?):
			O que fazer se o dado já existir no GCS: `"raise"` dispara erro de conflito;
			`"replace"` substitui o dado; `"pass"` não faz nada. Por padrão, é `"replace"`.
	"""
	log(f"Fazendo upload de '{path}' para 'gs://{bucket_name}/{blob_prefix}'")
	client = storage.Client()
	bucket = client.get_bucket(bucket_name)

	if if_exists not in ["raise", "replace", "pass"]:
		raise ValueError(
			f"Valor para `if_exist`, '{if_exists}', inválido;"
			"use 'raise', 'replace' ou 'pass'."
		)

	# Upload de um único arquivo
	if os.path.isfile(path):
		blob_name = os.path.basename(path)
		if blob_prefix:
			blob_name = f"{blob_prefix}/{blob_name}"
		blob = bucket.blob(blob_name)

		# Se o arquivo já existe
		if blob.exists():
			if if_exists == "pass":
				return
			if if_exists == "raise":
				raise FileExistsError(
					f"Arquivo '{blob_name}' já existe no bucket '{bucket_name}'!"
				)
		# Se estamos aqui, ou não existe arquivo, ou tudo bem substituí-lo
		blob.upload_from_filename(path)
		log(f"Upload de '{path}' terminado")
		return

	# Upload de uma pasta inteira
	if os.path.isdir(path):
		for root, _, files in os.walk(path):
			for file in files:
				file_path = os.path.join(root, file)
				blob_name = os.path.relpath(file_path, path)
				if blob_prefix:
					blob_name = f"{blob_prefix}/{blob_name}"
				blob = bucket.blob(blob_name)

				# Se o arquivo já existe
				if blob.exists():
					if if_exists == "pass":
						continue
					if if_exists == "raise":
						raise FileExistsError(
							f"Arquivo '{blob_name}' já existe no bucket '{bucket_name}'!"
						)

				# Se estamos aqui, ou não existe arquivo, ou tudo bem substituí-lo
				blob.upload_from_filename(file_path)
		log(f"Upload de '{path}' terminado")
		return

	raise ValueError(f"Caminho '{path}' não é nem diretório, nem arquivo!")


@task
def upload_to_cloud_storage_task(
	path: str,
	bucket_name: str,
	blob_prefix: str = None,
	if_exists: Literal["raise", "replace", "pass"] = "replace",
) -> None:
	"""
	Faz upload de arquivo ou pasta para o Google Cloud Storage

	Args:
		path (str):
			Caminho do arquivo ou pasta a ser enviado.
		bucket_name (str):
			Nome do bucket no Google Cloud Storage.
		blob_prefix (str?):
			Caminho no bucket para o arquivo. Por padrão, é `None`.
		if_exists (str?):
			O que fazer se o dado já existir no GCS: `"raise"` dispara erro de conflito;
			`"replace"` substitui o dado; `"pass"` não faz nada. Por padrão, é `"replace"`.
	"""
	return upload_to_cloud_storage(
		path, bucket_name, blob_prefix=blob_prefix, if_exists=if_exists
	)
