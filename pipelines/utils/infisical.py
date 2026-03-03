# -*- coding: utf-8 -*-
import base64
import os

from .logger import log
from .env import getenv_or_action

# from infisical import InfisicalClient
from infisical_sdk import InfisicalSDKClient as InfisicalClient
from prefect.context import FlowRunContext


def get_flow_run_mode() -> str:
	"""
	Returns the mode of the current flow run (either "prod" or "staging").
	"""
	# TODO: conferir isso aqui \/
	project_name = FlowRunContext.get("project_name") or "dev"
	if project_name not in ["production", "staging", "dev"]:
		raise ValueError(f"Invalid project name: {project_name}")
	if project_name == "production":
		return "prod"
	elif project_name == "staging":
		return "staging"
	else:
		return "dev"


def get_infisical_client() -> InfisicalClient:
	"""
	Returns an Infisical client using the default settings from environment variables.

	Returns:
		InfisicalClient: The Infisical client.
	"""
	token = getenv_or_action("INFISICAL_TOKEN", action="raise")
	site_url = getenv_or_action("INFISICAL_ADDRESS", action="raise")
	log(f"INFISICAL_ADDRESS: {site_url}")
	return InfisicalClient(
		token=token,
		host=site_url,
	)


def inject_env(
	secret_name: str,
	environment: str = None,
	path: str = "/",
	client: InfisicalClient = None,
) -> None:
	"""
	Loads the secret with the given name from Infisical into an environment variable.

	Args:
		secret_name (str): The name of the secret to retrieve.
		environment (str): The environment to retrieve the secret from.
		type (Literal["shared", "personal"], optional): The type of secret to retrieve.
			Defaults to "personal".
		path (str, optional): The path to retrieve the secret from. Defaults to "/".
		client (InfisicalClient, optional): The Infisical client to use. Defaults to None.
	"""
	if client is None:
		client = get_infisical_client()

	if not environment:
		environment = get_flow_run_mode() or environment
	log(f"Getting secret: {secret_name}")
	secret_value = client.secrets.get_secret_by_name(
		project_slug="rj-sms-dev",  # FIXME
		secret_name=secret_name,
		environment_slug=environment,
		secret_path=path
	).secretValue

	os.environ[secret_name] = secret_value


def inject_bd_credentials(environment: str = "dev", force_injection=False) -> None:
	"""
	Loads Base dos Dados credentials from Infisical into environment variables.

	Args:
		environment (str, optional): The infiscal environment for which to retrieve credentials. Defaults to 'dev'. Accepts 'dev' or 'prod'.

	Returns:
		None
	"""
	# Verify if all environment variables are already set
	all_variables_set = True
	for variable in [
		"BASEDOSDADOS_CONFIG",
		"BASEDOSDADOS_CREDENTIALS_PROD",
		"BASEDOSDADOS_CREDENTIALS_STAGING",
		"GOOGLE_APPLICATION_CREDENTIALS",
	]:
		if not os.environ.get(variable):
			all_variables_set = False
			break

	# If all variables are set, skip injection
	if all_variables_set and not force_injection:
		# log("All environment variables are already set. Skipping injection.")
		return

	# Else inject the variables
	client = get_infisical_client()

	log(f"ENVIROMENT: {environment}")
	for secret_name in [
		"BASEDOSDADOS_CONFIG",
		"BASEDOSDADOS_CREDENTIALS_PROD",
		"BASEDOSDADOS_CREDENTIALS_STAGING",
	]:
		inject_env(
			secret_name=secret_name,
			environment=environment,
			client=client,
		)

	# Create service account file for Google Cloud
	service_account_name = "BASEDOSDADOS_CREDENTIALS_PROD"
	credentials = base64.b64decode(os.environ[service_account_name])

	if not os.path.exists("/tmp"):
		os.makedirs("/tmp")

	with open("/tmp/credentials.json", "wb") as credentials_file:
		credentials_file.write(credentials)
	os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"


def get_secret(
	secret_name: str,
	environment: str = None,
	path: str = "/",
	client: InfisicalClient = None,
) -> dict:
	"""
	Returns the secret with the given name from Infisical.

	Args:
		secret_name (str): The name of the secret to retrieve.
		environment (str): The environment to retrieve the secret from.
		type (Literal["shared", "personal"], optional): The type of secret to retrieve. Defaults to
			"personal".
		path (str, optional): The path to retrieve the secret from. Defaults to "/".
		client (InfisicalClient, optional): The Infisical client to use. Defaults to None.

	Returns:
		str: The value of the secret.
	"""
	if client is None:
		client = get_infisical_client()

	if not environment:
		environment = get_flow_run_mode() or environment
	secret = client.secrets.get_secret_by_name(
		secret_name=secret_name,
		environment_slug=environment,
		secret_path=path,
	).secretValue
	return { secret_name: secret }
