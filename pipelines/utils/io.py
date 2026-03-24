# -*- coding: utf-8 -*-
import csv
import os
import shutil
import sys

import gspread
import pandas as pd

from pipelines.utils.cleanup import remove_column_accents
from pipelines.utils.infisical import get_credentials_from_env
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


@task()
def download_google_sheets(
	url: str, file_path: str, file_name: str, gsheets_sheet_name: str, csv_delimiter: str = ";"
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
