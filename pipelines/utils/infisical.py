# -*- coding: utf-8 -*-
import base64
import json
import os
from typing import List

from .logger import log
from .env import (
  get_current_environment,
  get_google_project_for_environment,
  getenv_or_action,
)

import basedosdados as bd

# from infisical import InfisicalClient
from infisical_sdk import InfisicalSDKClient as InfisicalClient
from google.oauth2 import service_account


def get_project():
  return getenv_or_action("INFISICAL_PROJECT_ID", action="raise")


def get_infisical_client() -> InfisicalClient:
  """
  Retorna instância de Infisical Client a partir das variáveis de ambiente
  `INFISICAL_ADDRESS` e `INFISICAL_TOKEN`
  """
  token = getenv_or_action("INFISICAL_TOKEN", action="raise")
  site_url = getenv_or_action("INFISICAL_ADDRESS", action="raise")
  return InfisicalClient(token=token, host=site_url)


def get_secret(secret_name: str, environment: str = None, path: str = "/") -> dict:
  """
  Obtém o secret no Infisical com nome e caminho especificados

  Args:
          secret_name (str): Nome do secret
          environment (str?):
                  Environment em que se deve buscar o secret. Por padrão, valor da
                  variável de ambiente `environment`
          path (str?): Caminho do secret; valor padrão "/"

  Returns:
          str: Valor do secret
  """
  client = get_infisical_client()
  environment = environment or get_current_environment()
  project_id = get_project()

  secret_value = client.secrets.get_secret_by_name(
    project_id=project_id,
    secret_path=path,
    secret_name=secret_name,
    environment_slug=environment,
  ).secretValue
  return secret_value


def inject_env(secret_name: str, environment: str = None, path: str = "/") -> None:
  """
  Carrega secret do Infisical em variável de ambiente de mesmo nome

  Args:
          secret_name (str): Nome do secret
          environment (str?):
                  Environment em que se deve buscar o secret. Por padrão, valor da
                  variável de ambiente `environment`
          path (str?): Caminho do secret; valor padrão "/"
  """
  secret_value = get_secret(secret_name=secret_name, environment=environment, path=path)
  os.environ[secret_name] = secret_value


def inject_bd_credentials(environment: str = "dev", force_injection=False) -> None:
  """
  Carrega credenciais de Base dos Dados do Infisical em variáveis de ambiente.

  Args:
          environment(str?):
                  Ambiente do Infiscal onde estão as credenciais, p.ex. "dev"/"prod".
                  Valor padrão de "dev".
          force_injection(bool?):
                  Caso todas as variáveis já estejam carregadas no ambiente, o servidor
                  do Infisical não é contactado, a não ser que a função receba
                  `force_injection=True`, situação em que elas são obtidas novamente.
                  Valor padrão de False.

  """
  # Confere se todas as variáveis já foram obtidas
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

  # Se já temos todas, não é necessário obtê-las novamente no Infisical
  if all_variables_set and not force_injection:
    return

  log(f"ENVIRONMENT: {environment}", level="debug")
  for secret_name in [
    "BASEDOSDADOS_CONFIG",
    "BASEDOSDADOS_CREDENTIALS_PROD",
    "BASEDOSDADOS_CREDENTIALS_STAGING",
  ]:
    inject_env(secret_name=secret_name, environment=environment)

  # Salva credenciais de conta de serviço para o Google Cloud
  service_account_name = (
    "BASEDOSDADOS_CREDENTIALS_PROD"
    if environment == "prod"
    else "BASEDOSDADOS_CREDENTIALS_STAGING"
  )
  credentials = base64.b64decode(os.environ[service_account_name])

  if not os.path.exists("/tmp"):
    os.makedirs("/tmp")

  with open("/tmp/credentials.json", "wb") as credentials_file:
    credentials_file.write(credentials)
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"

  project = get_google_project_for_environment(environment=environment)
  bd.config.from_file = True
  bd.config.project_config_path = "/tmp"
  bd.config.billing_project_id = project

  # Se não existir um config.toml, o basedosdados acha uma ótima ideia abrir
  # um prompt interativo pra configurar o essas infos, o que trava o container
  with open("/tmp/config.toml", "w") as config_file:
    config_file.write(f"""
bucket_name = "{project}"
[gcloud-projects]
  [gcloud-projects.staging]
  name = "rj-sms-dev"
  credentials_path = "/tmp/credentials.json"
  [gcloud-projects.prod]
  name = "rj-sms"
  credentials_path = "/tmp/credentials.json"
[api]
url = "https://staging.api.basedosdados.org/api/v1/graphql"
""")


def get_credentials_from_env(scopes: List[str] = None) -> service_account.Credentials:
  """
  Obtém credenciais das variáveis de ambiente
  """
  env: str = os.getenv("BASEDOSDADOS_CREDENTIALS_PROD", "")
  if env == "":
    log(
      "Credenciais de prod não estão carregadas; usando credenciais de dev",
      level="warning",
    )
    env = os.getenv("BASEDOSDADOS_CREDENTIALS_STAGING", "")
    if env == "":
      raise ValueError(
        "Variáveis BASEDOSDADOS_CREDENTIALS_PROD e _STAGING não configuradas!"
      )
  info: dict = json.loads(base64.b64decode(env))
  cred: service_account.Credentials = (
    service_account.Credentials.from_service_account_info(info)
  )
  if scopes:
    cred = cred.with_scopes(scopes)
  return cred
