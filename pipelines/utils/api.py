# -*- coding: utf-8 -*-
import datetime
import requests

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
