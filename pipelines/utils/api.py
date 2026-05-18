# -*- coding: utf-8 -*-
import datetime
import json
import requests

import google.auth.transport.requests
import google.oauth2.id_token
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.utils.datetime import now
from pipelines.utils.logger import log


def GET(
  url: str, headers: dict = None, retries: int = 2, retry_on: list[int] = None
) -> requests.Response | None:
  """Faz requisição para uma API utilizando o método GET."""
  session = requests.Session()
  retry_config = Retry(total=retries, backoff_factor=1, status_forcelist=retry_on)
  session.mount("https://", HTTPAdapter(max_retries=retry_config))
  try:
    response = session.get(url=url, headers=headers)
  except requests.exceptions.RequestException as e:
    log(e, level="error")
    return None
  return response


def cloud_function_request(
  url: str,
  credential: dict | None,
  request_type: str = "GET",
  body_params=None,
  query_params: dict = None,
  header_params: dict = None,
  env: str = "dev",
  api_type: str = "json",
  endpoint_for_filename: str = None,
  timeout: int = 90,
) -> dict:
  """
  Faz uma requisição via Cloud Function para endpoints acessíveis por IP fixo.
  """
  if env not in ["prod", "dev", "staging"]:
    raise ValueError("env must be 'prod' or 'dev'")

  cloud_function_url = "https://us-central1-rj-sms-dev.cloudfunctions.net/vitacare"
  request = google.auth.transport.requests.Request()
  token = google.oauth2.id_token.fetch_id_token(request, cloud_function_url)

  query_params_for_cf = {} if query_params is None else query_params.copy()
  if endpoint_for_filename:
    query_params_for_cf["_endpoint_for_filename"] = endpoint_for_filename

  payload = {
    "tipo_api": api_type,
    "url": url,
    "request_type": request_type,
    "body_params": body_params,
    "query_params": query_params_for_cf,
    "header_params": header_params,
    "credential": credential,
  }
  if isinstance(body_params, dict) and api_type == "json":
    payload["body_params"] = json.dumps(body_params)

  headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}

  try:
    response = requests.request(
      "POST", cloud_function_url, headers=headers, data=json.dumps(payload), timeout=timeout
    )
    if response.status_code != 200:
      message = f"[Cloud Function] Request failed: {response.status_code} - {response.reason}"
      log(message, level="error")
      raise RuntimeError(message)

    log("[Cloud Function] Request was successful")
    response_payload = response.json()
    if "gcs_url" in response_payload:
      gcs_url = response_payload["gcs_url"]
      log(f"[Cloud Function] GCS URL received. Downloading from: {gcs_url}")
      path_parts = gcs_url.replace("gs://", "").split("/", maxsplit=1)
      if len(path_parts) < 2:
        raise ValueError(f"Invalid GCS URL format: {gcs_url}")
      bucket_name, blob_name = path_parts
      blob = storage.Client().bucket(bucket_name).blob(blob_name)
      downloaded_content = blob.download_as_text()
      response_payload["body"] = (
        json.loads(downloaded_content) if api_type == "json" else downloaded_content
      )

    if response_payload["status_code"] != 200:
      log(
        f"[Target Endpoint] Request failed: {response_payload['status_code']} - {response_payload['body']}",
        level="error",
      )
    else:
      log("[Target Endpoint] Request was successful")
    return response_payload

  except requests.exceptions.Timeout:
    raise
  except Exception as e:
    raise RuntimeError(f"[Cloud Function] Request failed with unknown error: {e}") from e


def convert_usd_to_brl(usd: float, default_rate: float = None) -> float:
  """
  Tenta obter a cotação mais atual do dólar e retornar o valor de US$`usd` em reais.
  Caso não seja possível contactar nenhuma API de cotação, e `default_rate` tenha
  sido definida, esta é usado como cotação. Caso contrário, dispara `RuntimeError`
  """
  agora = now()
  ontem = agora - datetime.timedelta(days=1)
  ontem_mmddyyyy = ontem.strftime("%m-%d-%Y")
  ontem_yyyymmdd = ontem.strftime("%Y-%m-%d")
  hoje_yyyymmdd = agora.strftime("%Y-%m-%d")

  ################
  # Opção 1: API do Banco Central
  log("Conferindo cotação do dólar via API do Banco Central...")
  API_BASE_URL = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"
  API_ENDPOINT = "/CotacaoDolarDia(dataCotacao=@dataCotacao)"
  API_PARAMS = f"?@dataCotacao='{ontem_mmddyyyy}'&$format=json"
  try:
    response = requests.get(url=f"{API_BASE_URL}{API_ENDPOINT}{API_PARAMS}")

    usd_to_brl_rate = float(response.json()["value"][0]["cotacaoCompra"])
    return usd * usd_to_brl_rate

  except requests.RequestException as e:
    log(f"Erro ao contactar API do Banco Central: {e}", level="error")
  except Exception as e:
    log(f"Erro ao obter cotação do Banco Central: {e}", level="error")

  ################
  # Opção 2: API de biblioteca online
  log("Conferindo cotação do dólar via `currency-api`...")
  API_BASE_URL = "https://cdn.jsdelivr.net/npm"
  API_ENDPOINT = "/@fawazahmed0/currency-api@latest/v1/currencies/usd.json"
  try:
    response = requests.get(url=f"{API_BASE_URL}{API_ENDPOINT}")
    json_resp = response.json()
    ref_date = json_resp["date"]
    if ref_date != hoje_yyyymmdd and ref_date != ontem_yyyymmdd:
      log(
        f"Data da cotação é '{ref_date}', diferente de hoje {hoje_yyyymmdd}",
        level="warning",
      )

    usd_to_brl_rate = float(json_resp["usd"]["brl"])
    return usd * usd_to_brl_rate

  except requests.RequestException as e:
    log(f"Erro ao contactar `currency-api`: {e}", level="error")
  except Exception as e:
    log(f"Erro ao obter cotação de `currency-api`: {e}", level="error")

  ################
  # Opçaõ 3: Cotação padrão
  if default_rate:
    return usd * default_rate

  ################
  # Opçaõ 4: Chorar pro usuário
  raise RuntimeError("Não foi possível descobrir a cotação do dólar!")
