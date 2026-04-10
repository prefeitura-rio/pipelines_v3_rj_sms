# -*- coding: utf-8 -*-
import csv
import datetime
import io
import os
import time
from typing import Iterator, List, Literal

import gspread
import pandas as pd
import requests

from google.oauth2 import service_account
from google.auth.transport import requests as google_requests
from google.cloud import storage
from google.cloud.storage.blob import Blob
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from pipelines.utils.cleanup import prettify_byte_size, cleanup_columns_for_bigquery
from pipelines.utils.datetime import from_relative_date
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
		gspread_client
		.open_by_url(url)
		.worksheet(gsheets_sheet_name)
		.get_values()
	)  # fmt: skip
	# Primeira linha contém cabeçalho
	new_header = dataframe.iloc[0]
	# Remove cabeçalho dos dados
	dataframe = dataframe[1:]
	# Redefine colunas como cabeçalho obtido anteriormente
	dataframe.columns = new_header

	log(f">>>>> Dataframe shape: {dataframe.shape}")
	log(f">>>>> Dataframe colunas (cruas):    {dataframe.columns}")
	dataframe.columns = cleanup_columns_for_bigquery(
		dataframe, lowercase=True, raise_on_repeats="raise"
	)
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


def build_bucket_name(bucket_name: str, environment: str) -> str:
	"""
	Monta o nome final do bucket com base no ambiente.

	Args:
		bucket_name (str): Nome base do bucket.
		environment (str): Ambiente atual da execução.

	Returns:
		str: Nome final do bucket.
	"""
	if environment in ["prod", "local-prod"]:
		resolved_bucket_name = bucket_name
	else:
		resolved_bucket_name = f"{bucket_name}_{environment}"

	log(f"Nome do Bucket final: '{resolved_bucket_name}'")
	return resolved_bucket_name


###########################
##      Google Drive     ##
###########################


def get_google_drive_service():
	"""
	Cria um cliente autenticado da API do Google Drive.

	Returns:
		Resource: Cliente autenticado do Google Drive.
	"""
	credentials = get_credentials_from_env(
		scopes=["https://www.googleapis.com/auth/drive.readonly"]
	)
	return build("drive", "v3", credentials=credentials)


def list_google_drive_files(
	folder_id: str, last_modified_date: str = None
) -> List[dict[str, str]]:
	"""
	Lista arquivos de uma pasta do Google Drive, incluindo subpastas.

	Args:
		folder_id (str): ID da pasta raiz no Google Drive.
		last_modified_date (str, optional): Data mínima de modificação para filtrar arquivos.

	Returns:
		List[dict[str, str]]: Lista de arquivos encontrados com metadados básicos.
	"""
	service = get_google_drive_service()
	modified_since = from_relative_date(last_modified_date)

	if isinstance(modified_since, datetime.datetime):
		modified_since = modified_since.date()

	root_folder = (
		service.files()
		.get(fileId=folder_id, fields="id, name", supportsAllDrives=True)
		.execute()
	)
	root_folder_name = root_folder["name"]

	def _list_files(
		current_folder_id: str, parent_path: str = ""
	) -> List[dict[str, str]]:
		files = []
		page_token = None

		while True:
			response = (
				service.files()
				.list(
					q=f"'{current_folder_id}' in parents and trashed = false",
					fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
					pageToken=page_token,
					supportsAllDrives=True,
					includeItemsFromAllDrives=True,
				)
				.execute()
			)

			for item in response.get("files", []):
				relative_path = (
					f"{parent_path}/{item['name']}" if parent_path else item["name"]
				)

				# Se for pasta, continua a busca dentro dela
				if item["mimeType"] == "application/vnd.google-apps.folder":
					files.extend(_list_files(item["id"], relative_path))
					continue

				modified_time = item["modifiedTime"]
				modified_date = datetime.datetime.fromisoformat(
					modified_time.replace("Z", "+00:00")
				).date()

				# Ignora arquivos mais antigos que a data informada
				if modified_since and modified_date < modified_since:
					continue

				files.append(
					{
						"id": item["id"],
						"name": item["name"],
						"relative_path": relative_path,
						"modified_time": modified_time,
					}
				)

			page_token = response.get("nextPageToken")
			if not page_token:
				break

		return files

	# Começa a listagem incluindo o nome da pasta raiz no caminho relativo
	items = _list_files(folder_id, root_folder_name)
	log(f"Encontrado(s) {len(items)} arquivo(s) no Google Drive")
	return items


def download_google_drive_file(file_id: str, destination_path: str = None) -> str:
	"""
	Baixa um arquivo do Google Drive para um caminho local.

	Args:
		file_id (str): ID do arquivo no Google Drive.
		destination_path (str, optional): Caminho local do arquivo ou pasta de destino.

	Returns:
		str: Caminho final do arquivo baixado.
	"""
	service = get_google_drive_service()
	file_metadata = (
		service.files()
		.get(fileId=file_id, fields="name", supportsAllDrives=True)
		.execute()
	)

	# Se nenhum destino for informado, cria um caminho temporário com o nome original
	if not destination_path:
		destination_path = os.path.join(create_tmp_data_folder(), file_metadata["name"])
	elif os.path.isdir(destination_path):
		destination_path = os.path.join(destination_path, file_metadata["name"])

	os.makedirs(os.path.dirname(destination_path), exist_ok=True)

	request = service.files().get_media(fileId=file_id)
	buffer = io.BytesIO()

	with open(destination_path, "wb") as output_file:
		downloader = MediaIoBaseDownload(buffer, request)

		done = False
		while not done:
			_, done = downloader.next_chunk()

		output_file.write(buffer.getvalue())

	return destination_path

###########################
##     Google CloudSQL   ##
###########################


def get_access_token(
	scopes: list[str] = None,
) -> str:
	"""
	Obtém um access token OAuth2 para autenticar chamadas na API do Cloud SQL.

	Args:
		scopes (list[str], optional): Escopos OAuth2 usados na autenticação.

	Returns:
		str: Access token válido para autenticação na API.
	"""
	if scopes is None:
		scopes = ["https://www.googleapis.com/auth/cloud-platform"]

	credentials = service_account.Credentials.from_service_account_file(
		"/tmp/credentials.json",
		scopes=scopes,
	)
	credentials.refresh(google_requests.Request())
	return credentials.token

def call_cloudsql_api(
	method: str,
	path: str,
	payload: dict = None,
	api_base_url: str = None,
):
	"""
	Faz uma chamada autenticada para a API Admin do Cloud SQL.

	Args:
		method (str): Método HTTP da requisição (GET, POST, DELETE, etc).
		path (str): Caminho da API relativo ao projeto (ex: instances, instances/{instance}).
		payload (dict, optional): Corpo JSON enviado na requisição.
		api_base_url (str, optional): URL base da API. Se não informado, usa o padrão.

	Returns:
		dict | list | None: Resposta JSON parseada, quando houver conteúdo.
	"""
	if api_base_url is None:
		api_base_url = "https://sqladmin.googleapis.com/sql/v1beta4/projects/rj-sms-dev"

	url = f"{api_base_url}/{path.lstrip('/')}"
	headers = {
		"Authorization": f"Bearer {get_access_token()}",
		"Content-Type": "application/json",
	}

	log(f"Chamando Cloud SQL Admin API: {method.upper()} {url}")
	response = requests.request(
		method=method.upper(),
		url=url,
		headers=headers,
		json=payload,
		timeout=60,
	)

	log(f"Cloud SQL Admin API respondeu com status {response.status_code}")

	try:
		response.raise_for_status()
	except requests.HTTPError:
		log(
			f"Erro na chamada da Cloud SQL Admin API: {response.text}",
			level="error",
		)
		raise

	if not response.content:
		return None

	return response.json()

def wait_for_operations(
	instance_name: str,
	max_attempts: int = 30,
	sleep_seconds: int = 15,
) -> None:
	"""
	Aguarda até que a operação mais recente de uma instância Cloud SQL termine.

	Args:
		instance_name (str): Nome da instância Cloud SQL.
		max_attempts (int, optional): Número máximo de tentativas de polling.
		sleep_seconds (int, optional): Intervalo em segundos entre tentativas.

	Returns:
		None
	"""
	log(f"Aguardando operações do Cloud SQL para a instância '{instance_name}'")

	for attempt in range(1, max_attempts + 1):
		response = call_cloudsql_api(
			method="GET",
			path=f"operations?instance={instance_name}&maxResults=1",
		)
		items = response.get("items", []) if response else []

		if not items:
			return

		operation = items[0]
		status = operation.get("status")
		operation_name = operation.get("name")

		log(f"Operação '{operation_name}' da instância '{instance_name}' está em '{status}'")

		if status == "DONE":
			return

		if attempt < max_attempts:
			time.sleep(sleep_seconds)

	log(
		f"Número máximo de tentativas atingido ao aguardar operações da instância "
		f"'{instance_name}'",
		level="warning",
	)


def get_instance_status(instance_name: str) -> dict:
	"""
	Obtém o status atual de uma instância Cloud SQL.

	Args:
		instance_name (str): Nome da instância Cloud SQL.

	Returns:
		dict: Estado atual e activation policy da instância.
	"""
	log(f"Consultando status da instância Cloud SQL '{instance_name}'")
	response = call_cloudsql_api(
		method="GET",
		path=f"instances/{instance_name}",
	)

	status = {
		"state": response.get("state"),
		"activation_policy": response.get("settings", {}).get("activationPolicy"),
	}

	log(
		f"Instância '{instance_name}' está em '{status['state']}' "
		f"com activation policy '{status['activation_policy']}'"
	)
	return status


def ensure_instance_running(instance_name: str) -> None:
	"""
	Garante que uma instância Cloud SQL esteja ligada.

	Args:
		instance_name (str): Nome da instância Cloud SQL.

	Returns:
		None
	"""
	status = get_instance_status(instance_name)
	if status["activation_policy"] == "ALWAYS" and status["state"] != "STOPPED":
		log(f"Instância '{instance_name}' já está em execução")
		return

	log(f"Ligando instância Cloud SQL '{instance_name}'")
	instance = call_cloudsql_api(
		method="GET",
		path=f"instances/{instance_name}",
	)
	settings_version = instance.get("settings", {}).get("settingsVersion")

	call_cloudsql_api(
		method="PATCH",
		path=f"instances/{instance_name}",
		payload={
			"settings": {
				"activationPolicy": "ALWAYS",
				"settingsVersion": settings_version,
			}
		},
	)
	wait_for_operations(instance_name)
	get_instance_status(instance_name)


def ensure_instance_stopped(instance_name: str) -> None:
	"""
	Garante que uma instância Cloud SQL esteja desligada.

	Args:
		instance_name (str): Nome da instância Cloud SQL.

	Returns:
		None
	"""
	status = get_instance_status(instance_name)
	if status["activation_policy"] == "NEVER" or status["state"] == "STOPPED":
		log(f"Instância '{instance_name}' já está parada")
		return

	log(f"Desligando instância Cloud SQL '{instance_name}'")
	instance = call_cloudsql_api(
		method="GET",
		path=f"instances/{instance_name}",
	)
	settings_version = instance.get("settings", {}).get("settingsVersion")

	call_cloudsql_api(
		method="PATCH",
		path=f"instances/{instance_name}",
		payload={
			"settings": {
				"activationPolicy": "NEVER",
				"settingsVersion": settings_version,
			}
		},
	)
	wait_for_operations(instance_name)
	get_instance_status(instance_name)
