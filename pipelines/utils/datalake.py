# -*- coding: utf-8 -*-

import glob
import os
from typing import Literal

import basedosdados as bd

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task(retries=3, retry_delay_seconds=60)
def upload_to_datalake(
	input_path: str,
	dataset_id: str,
	table_id: str,
	dump_mode: Literal["append", "overwrite"] = "append",
	delete_mode: Literal["staging", "all"] = "all",
	source_format: Literal["csv", "parquet"] = "csv",
	csv_delimiter: str = ";",
	if_exists: str = "replace",
	if_storage_data_exists: str = "replace",
	biglake_table: bool = True,
	dataset_is_public: bool = False,
	exception_on_missing_input_file: bool = False,
):
	"""
	Faz upload de arquivo para um bucket do Google Cloud Storage;
	cria ou adiciona dados a uma tabela do BigQuery.

	Args:
		input_path (str):
			The path to the input data file. It can be a folder or a file.
		dataset_id (str):
			The ID of the BigQuery dataset.
		table_id (str):
			The ID of the BigQuery table.
		dump_mode (str?):
			The dump mode for the table. Defaults to "append". Accepted values are "append" and "overwrite".
		delete_mode (str?):
			Whether to delete both `_staging` and prod tables ("all"), or only `_staging` ("staging").
			Defaults to "all".
		source_format (str?):
			The format of the input data. Defaults to "csv". Accepted values are "csv" and "parquet".
		csv_delimiter (str?):
			The delimiter used in the CSV file. Defaults to ";".
		if_exists (str?):
			The behavior if the table already exists. Defaults to "replace".
		if_storage_data_exists (str?):
			The behavior if the storage data already exists. Defaults to "replace".
		biglake_table (bool?):
			Whether the table is a BigLake table. Defaults to True.
		dataset_is_public (bool?):
			Whether the dataset is public. Defaults to False.
		exception_on_missing_input_file (bool?):
			If True, raises `FileNotFoundError` if `input_path` is an empty string or
			directory with no files. Defaults to False.

	Raises:
		RuntimeError: If an error occurs during the upload process.

	Returns:
		None
	"""
	if input_path == "":
		log("`input_path` vazio; nada para fazer upload", level="warning")
		if exception_on_missing_input_file:
			raise FileNotFoundError(f"Nenhum arquivo em '{input_path}'")
		return

	if os.path.isdir(input_path):
		log(f"`input_path` é diretório: '{input_path}'")

		reference_path = os.path.join(input_path, f"**/*.{source_format}")
		log(f"Procurando por arquivos '{reference_path}'")

		if len(glob.glob(reference_path, recursive=True)) == 0:
			log(f"Nenhum arquivo '{source_format}' encontrado em '{input_path}'", level="warning")
			if exception_on_missing_input_file:
				raise FileNotFoundError(f"Nenhum arquivo em '{input_path}'")
			return

	tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
	table_staging = f"{tb.table_full_name['staging']}"

	st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
	storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
	storage_path_link = (
		f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
		f"/staging/{dataset_id}/{table_id}"
	)
	log(
		f"Fazendo upload de arquivo '{input_path}' para '{storage_path}' com formato '{source_format}'"
	)

	try:
		# Se tabela não existe, cria
		table_exists = tb.table_exists(mode="staging")
		if not table_exists:
			log(f"CRIANDO TABELA: '{dataset_id}.{table_id}'")
			tb.create(
				path=input_path,
				source_format=source_format,
				csv_delimiter=csv_delimiter,
				if_storage_data_exists=if_storage_data_exists,
				biglake_table=biglake_table,
				dataset_is_public=dataset_is_public,
			)
			log("Upload para o BigQuery bem sucedido")
			return

		# Se a tabela já existe, e queremos dar `append`
		if dump_mode == "append":
			log(f"TABELA EXISTE; FAZENDO APPEND: '{dataset_id}.{table_id}'")
			tb.append(filepath=input_path, if_exists=if_exists)
			log("Upload para o BigQuery bem sucedido")
			return

		# Se a tabela já existe, e queremos substituí-la
		if dump_mode == "overwrite":
			# Apaga GCS
			log(
				"OVERWRITE: TABELA EXISTE; APAGANDO DADOS ANTIGOS DO GCS\n"
				f"'{storage_path}'\n"
				f"'{storage_path_link}'"
			)
			st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
			log(
				f"OVERWRITE: DADOS ANTIGOS APAGADOS DO GCS\n'{storage_path}'\n'{storage_path_link}'"
			)

			# Apaga tabela do BigQuery
			tb.delete(mode=("staging" if delete_mode == "staging" else "all"))
			if delete_mode == "staging":
				log(f"OVERWRITE: TABELA APAGADA DO BIGQUERY:\n{table_staging}\n")
			else:
				deleted_tables = [
					tb.table_full_name[key] for key in tb.table_full_name.keys() if key != "all"
				]
				log(f"OVERWRITE: TABELAS APAGADAS DO BIGQUERY:\n{deleted_tables}\n")

			# Cria tabela agora que a anterior foi apagada
			tb.create(
				path=input_path,
				source_format=source_format,
				csv_delimiter=csv_delimiter,
				if_storage_data_exists=if_storage_data_exists,
				biglake_table=biglake_table,
				dataset_is_public=dataset_is_public,
			)
			log("Upload para o BigQuery bem sucedido")
			return

		raise ValueError(f"`dump_mode` '{dump_mode}' desconhecido!")

	except Exception as e:  # pylint: disable=W0703
		log(f"An error occurred: {e}", level="error")
		raise RuntimeError() from e
