# -*- coding: utf-8 -*-
from os import getenv

from prefect.context import FlowRunContext

from pipelines.constants import constants

from .logger import log


def environment_is_valid(environment: str = None, raise_if_not: bool = True):
	"""
	Garante que o valor de environment é válido; permite que um erro
	seja disparado caso não
	"""
	if environment not in constants.ALLOWED_ENVIRONMENTS.value:
		if raise_if_not:
			raise ValueError(f"'{environment}' não é valor permitido para `environment`!")
		return False
	return True


def get_current_environment() -> str:
	"""
	Retorna o valor da variável de ambiente `environment`, se possuir
	valor válido; caso contrário, dá erro
	"""
	environment = None
	# Tenta pegar o contexto da Flow Run atual
	fr_ctx = FlowRunContext.get()
	if fr_ctx:
		# Se conseguiu, tenta pegar o parâmetro `environment`
		# passado para o flow
		environment = fr_ctx.parameters.get("environment")
	# Se não conseguiu o contexto, ou não existe/não foi preenchido
	# o parâmetro `environment`
	if not fr_ctx or not environment:
		# Pega da variável de ambiente, que é passada no deploy
		# e depende se o flow é de prod ou staging
		environment = getenv_or_action("environment", action="raise")
	# Garante que o environment é válido antes de retornar
	environment_is_valid(environment=environment)
	return environment


def get_google_project_for_environment(environment: str = None):
	"""
	Retorna `"rj-sms"` para environment `"prod"`, `"rj-sms-dev"` para
	environment `"dev"`, ...
	"""
	if not environment:
		environment = get_current_environment()
	else:
		environment_is_valid(environment=environment)

	project = constants.GOOGLE_CLOUD_PROJECT.value.get(environment)
	if not project:
		raise ValueError(f"Não existe projeto definido para environment '{environment}'")

	return project


def getenv_or_action(key: str, *, default: str = None, action: str = "raise"):
	"""
	Retorna o valor da variável de ambiente com uma dada chave, o valor padrão,
	ou o executa uma determinada ação caso nenhum desses tenha sido definido

	Args:
		key (str): O nome da variável de ambiente
		default (str, optional):
			Valor padrão a ser usado caso a variável não tenha sido definida
		action (str?):
			O nome da ação a executar caso nem a variável de ambiente exista, nem o
			valor padrão tenha sido definido. Valores possíveis são: `"raise"`,
			`"warn"` e `"ignore"`; o valor `"raise"` é o padrão

	Raises:
		ValueError:
			* Caso `action` não seja um dos valores permitidos;
			* Caso `action` seja `"raise"` e nem a variável de ambiente, nem
			o valor padrão tenham sido definidos

	Returns:
		str:
			O valor da variável de ambiente, ou o valor padrão dado caso ela não
			tenha sido definida
	"""
	if action not in ["raise", "warn", "ignore"]:
		raise ValueError(f"Invalid action: '{action}'")

	value = getenv(key, default)

	if value is None:
		if action == "raise":
			raise ValueError(f"Environment variable '{key}' is not set")
		elif action == "warn":
			log(f"WARNING: Environment variable '{key}' is not set", level="warning")
		elif action == "ignore":
			pass

	return value


def get_prefect_url():
	return getenv_or_action("PREFECT_API_URL").removesuffix("/api")
