# -*- coding: utf-8 -*-
import csv
import os
from typing import Iterator, List

import gspread
import pandas as pd

from google.cloud import storage
from google.cloud.storage.blob import Blob

from pipelines.utils.cleanup import remove_column_accents
from pipelines.utils.infisical import get_credentials_from_env
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task()
def download_google_sheets(
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
	dataframe.columns = remove_column_accents(dataframe)
	log(f">>>>> Dataframe colunas (tratadas): {dataframe.columns}")

	dataframe.to_csv(
		filepath, index=False, sep=csv_delimiter, encoding="utf-8", quoting=csv.QUOTE_ALL
	)


def download_from_bucket(
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
