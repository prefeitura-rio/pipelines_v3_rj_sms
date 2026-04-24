# -*- coding: utf-8 -*-
from pandas import DataFrame, DateOffset

from pipelines.utils.api import GET
from pipelines.utils.datetime import current_year, now, now_str
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def format_month(month: str, year: str) -> str:
  """
  Formata string numérica referente ao mês-ano.

  Se a execução estiver ocorrendo no início do mês (dia < 7), a função irá formatar o mês-ano para o mês anterior,
  garantindo que o mês anterior foi completamente extraído e carregado no datalake, evitando possíveis lacunas nos dados.
  """
  if not month and not year:
    current_date = now()
    # Se for início do mês, a extração será para o mês anterior
    # Em testes realizados, no começo do mês a API do SICLOM ainda não possui os dados do mês corrente,
    # mas já possui os dados do mês anterior completamente disponíveis, por isso essa lógica foi implementada
    # para garantir a extração dos dados mais recentes possíveis.
    if current_date.day <= 7:
      target_month = current_date + DateOffset(months=-1)
      target_date = target_month.strftime("%m/%Y")
    else:
      target_date = current_date.strftime("%m/%Y")
    return target_date

  if int(month) > 12:
    raise ValueError(f"Mês '{month}' é inválido!")
  if int(year) > current_year() or int(year) < 1950:
    raise ValueError(f"Ano '{year}' é inválido!")

  return f"{str(month).zfill(2)}/{year}"


@task
def generate_formatted_months(reference_year: str, interval) -> list[str]:
  """
  Gera strings numéricas formatadas referente aos meses para extrações anuais
  de forma regressiva a partir do ano de referência, seguindo o formato MM/YYYY.
  """
  periods = []
  for year in range(reference_year, reference_year - interval, -1):
    periods.extend([f"{str(month).zfill(2)}/{year}" for month in range(1, 13)])
  log(periods)
  return periods


@task
def get_siclom_period_data(
  base_url: str, endpoint: str, api_key: str, period: str
) -> DataFrame:
  """Faz a requisição para a API do SICLOM utilizando a busca por mês e ano."""
  log(f"Buscando dados de {period}...")

  headers = {"Accept": "application/json", "X-API-KEY": api_key}
  url = f"{base_url}{endpoint}{period}"
  response = GET(url=url, headers=headers, retry_on=[104, 502, 503, 504])
  # TODO: esse é o comportamento esperado para erros na API? Ou raise RuntimeError()?
  if not response:
    log(f"Erro ao requisitar URL '{url}'", level="error")
    return DataFrame()
  if response.status_code != 200:
    log(f"URL '{url}' retornou status '{response.status_code}'", level="error")
    return DataFrame()

  data = response.json()["resultado"]
  df = DataFrame(data)
  df["extracted_at"] = now_str()
  log("✅ Extração realizada com sucesso!")
  return df


@task
def get_siclom_prep_data(
  base_url: str, endpoint: str, api_key: str, period: str
) -> DataFrame:
  """
  Faz a requisição para a API do SICLOM utilizando a busca por mês e ano para dados de PREP.
  Este endpoint é paginado e possui uma lógica de extração diferente dos demais, por isso foi
  necessário criar uma task específica para ele.
  """
  log(f"Buscando dados de PREP de {period}...")

  headers = {"Accept": "application/json", "X-API-KEY": api_key}

  url = f"{base_url}{endpoint}{period}?page=1&numItemsPerPage=50"
  extracted_data = []
  response = GET(url=url, headers=headers, retry_on=[104, 502, 503, 504])

  if not response:
    return DataFrame()

  payload = response.json()
  if "resultado" not in payload:
    log("Nenhum dado retornado para o período solicitado.", level="warning")
    return DataFrame()
  page = payload["resultado"]
  extracted_data.extend(page["items"])
  next = page["next"]

  log("Iniciando extração por paginação...")
  while next:
    log(f"Extraindo página {page['current']}/{page['pageCount']}...")
    url = f"{base_url}{endpoint}{period}?page={next}&numItemsPerPage=50"
    response = GET(url=url, headers=headers, retry_on=[104, 502, 503, 504])
    if not response or response.status_code != 200:
      break
    payload = response.json()
    if "resultado" not in payload:
      break
    page = payload["resultado"]
    extracted_data.extend(page["items"])
    if "next" not in page:
      break
    next = page["next"]

  df = DataFrame(extracted_data)
  df["extracted_at"] = now_str()
  return df
