# -*- coding: utf-8 -*-
import re
from time import sleep

import requests
from google.auth.transport import requests as google_requests
from google.oauth2 import service_account

from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants
from pipelines.utils.logger import log


###########################
##      Validation       ##
###########################


def validate_restore_input(item: dict) -> None:
	"""
	Valida os campos mínimos para restore.

	Args:
		item (dict): Item da etapa.

	Returns:
		None
	"""
	source_uri = item.get("source_uri")
	database_name = item.get("database_name")

	if not source_uri:
		raise ValueError("Item sem 'source_uri'.")

	if not database_name:
		raise ValueError("Item sem 'database_name'.")

	if not source_uri.lower().endswith(".bak"):
		raise ValueError(f"Arquivo inválido: '{source_uri}'. Esperado '.bak'.")


def validate_database_name(database_name: str) -> None:
	"""
	Valida nome da database.

	Args:
		database_name (str): Nome da database.

	Returns:
		None
	"""
	if database_name in ["master", "model", "msdb", "tempdb"]:
		raise PermissionError(f"Database '{database_name}' é reservada.")

	if re.search(r"[^A-Za-z0-9_\-]", database_name):
		raise PermissionError(f"Database '{database_name}' contém caracteres inválidos.")


###########################
##      Result utils     ##
###########################


def build_gcs_to_cloudsql_result(item: dict, status: str, error_detail: str = None) -> dict:
	"""
	Monta resultado do item.

	Args:
		item (dict): Item original.
		status (str): Status final.
		error_detail (str, optional): Detalhe do erro.

	Returns:
		dict
	"""
	return {
		"source_uri": item.get("source_uri"),
		"database_name": item.get("database_name"),
		"status": status,
		"error_detail": error_detail,
		"metadata": item.get("metadata") or {},
	}


def build_gcs_to_cloudsql_summary(items: list[dict]) -> dict:
	"""
	Resume execução da etapa.

	Args:
		items (list[dict]): Resultados individuais.

	Returns:
		dict
	"""
	return {
		"total_items": len(items),
		"total_successful": sum(1 for i in items if i["status"] == "success"),
		"total_failed": sum(1 for i in items if i["status"] == "failed"),
		"items": items,
	}


###########################
##     Cloud SQL API     ##
###########################


def get_access_token() -> str:
	"""
	Gera token de acesso para API do Cloud SQL.

	Returns:
		str
	"""
	credentials = service_account.Credentials.from_service_account_file(
		constants.SERVICE_ACCOUNT_FILE.value,
		scopes=["https://www.googleapis.com/auth/cloud-platform"],
	)
	credentials.refresh(google_requests.Request())
	return credentials.token


def call_cloudsql_api(method: str, url_path: str, json: dict = None) -> dict:
	"""
	Chama API do Cloud SQL com retry para conflito (409).

	Args:
		method (str): Método HTTP.
		url_path (str): Caminho da API.
		json (dict, optional): Payload.

	Returns:
		dict
	"""
	method = (method or "GET").upper()

	access_token = get_access_token()
	headers = {
		"Authorization": f"Bearer {access_token}",
		"Content-Type": "application/json",
	}

	api_url = f"{constants.API_BASE.value}{url_path}"

	for attempt in range(25):
		response = requests.request(method, api_url, headers=headers, json=json)

		if response.status_code < 400:
			return response.json() if response.content else {}

		if response.status_code == 409:
			log("(call_cloudsql_api) operação em andamento; tentando novamente...")
			sleep(15)
			continue

		log(response.text, level="error")
		response.raise_for_status()

	raise Exception("Falha ao chamar API do Cloud SQL após várias tentativas.")


def wait_for_operations(instance_name: str) -> None:
	"""
	Espera operação mais recente finalizar.

	Args:
		instance_name (str)

	Returns:
		None
	"""
	url_path = f"/operations?instance={instance_name}&maxResults=1"

	for _ in range(40):
		log("(wait_for_operations) verificando se ainda existe alguma operação em andamento...")
		response = call_cloudsql_api("GET", url_path)
		operations = response.get("items", [])

		if not operations:
			return

		status = operations[0].get("status")

		if status == "DONE":
			return

		log("(wait_for_operations) aguardando operação finalizar...")
		sleep(15)

	log("(wait_for_operations) tempo máximo atingido", level="warning")


def get_instance_status(instance_name: str) -> dict:
	"""
	Retorna estado da instância.

	Args:
		instance_name (str)

	Returns:
		dict
	"""
	response = call_cloudsql_api("GET", f"/instances/{instance_name}")

	state = response.get("state")
	activation_policy = response.get("settings", {}).get("activationPolicy")

	log(f"(get_instance_status) state={state} policy={activation_policy}")

	return {
		"state": state,
		"activation_policy": activation_policy,
	}


def database_exists(instance_name: str, database_name: str) -> bool:
	"""
	Confere se database existe.

	Args:
		instance_name (str)
		database_name (str)

	Returns:
		bool
	"""
	response = call_cloudsql_api("GET", f"/instances/{instance_name}/databases")

	for db in response.get("items", []):
		if db.get("name") == database_name:
			return True

	return False


###########################
##   Instance control    ##
###########################


def ensure_instance_running(instance_name: str) -> None:
	"""
	Liga instância.

	Args:
		instance_name (str)

	Returns:
		None
	"""
	wait_for_operations(instance_name)

	log("(ensure_instance_running) ligando instância...")
	payload = {"settings": {"activationPolicy": "ALWAYS"}}

	call_cloudsql_api("PATCH", f"/instances/{instance_name}", json=payload)

	wait_for_operations(instance_name)
	get_instance_status(instance_name)


def ensure_instance_stopped(instance_name: str) -> None:
	"""
	Desliga instância.

	Args:
		instance_name (str)

	Returns:
		None
	"""
	wait_for_operations(instance_name)

	log("(ensure_instance_stopped) desligando instância...")
	payload = {"settings": {"activationPolicy": "NEVER"}}

	call_cloudsql_api("PATCH", f"/instances/{instance_name}", json=payload)

	wait_for_operations(instance_name)
	get_instance_status(instance_name)


###########################
##   Restore helpers     ##
###########################


def build_import_payload(source_uri: str, database_name: str) -> dict:
	"""
	Monta payload de import.

	Args:
		source_uri (str)
		database_name (str)

	Returns:
		dict
	"""
	return {
		"importContext": {
			"fileType": "BAK",
			"uri": source_uri,
			"database": database_name,
		}
	}


def delete_database(instance_name: str, database_name: str) -> None:
	"""
	Apaga database antes do restore.

	Args:
		instance_name (str)
		database_name (str)

	Returns:
		None
	"""
	log(f"(delete_database) deletando database '{database_name}'...")

	try:
		call_cloudsql_api(
			"DELETE",
			f"/instances/{instance_name}/databases/{database_name}",
		)
	except requests.HTTPError as exc:
		if exc.response is not None and exc.response.status_code == 404:
			log(
				f"(delete_database) database '{database_name}' não encontrada; seguindo com o import..."
			)
			return
		raise

	wait_for_operations(instance_name)


def import_backup_to_database(instance_name: str, source_uri: str, database_name: str) -> None:
	"""
	Importa backup .bak.

	Args:
		instance_name (str)
		source_uri (str)
		database_name (str)

	Returns:
		None
	"""
	log(f"(import_backup_to_database) importando backup para '{database_name}'...")

	payload = build_import_payload(source_uri, database_name)

	call_cloudsql_api(
		"POST",
		f"/instances/{instance_name}/import",
		json=payload,
	)

	wait_for_operations(instance_name)
