# -*- coding: utf-8 -*-
import json
from typing import Dict, List, Tuple

import requests
from pandas import DataFrame

from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.datetime import (
  from_relative_date,
  now_naive,
  parse_date_or_today,
  today_str,
)
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import RecursosRMD, resource_to_table_map


@task
def get_secrets(recurso: RecursosRMD, environment: str):
  api_url = get_secret(secret_name="API_URL", path="/rmd", environment=environment)
  consumer_endpoint = get_secret(
    secret_name="CONSUMER_ENDPOINT", path="/rmd", environment=environment
  )
  api_full_url = f"{api_url}{consumer_endpoint}"

  resource_id = get_secret(secret_name=recurso, path="/rmd", environment=environment)

  consumer_uuid = get_secret(
    secret_name="CONSUMER_UUID", path="/rmd", environment=environment
  )
  consumer_key = get_secret(
    secret_name="CONSUMER_KEY", path="/rmd", environment=environment
  )
  return {
    "url": api_full_url,
    "resource": resource_id,
    "uuid": consumer_uuid,
    "key": consumer_key,
    "table_id": resource_to_table_map[recurso],
  }


@task
def calculate_date_interval(data_inicio: str, data_fim: str | None) -> Tuple[str, str]:
  start_date = from_relative_date(data_inicio).isoformat()

  if not data_fim:
    return (start_date, today_str())

  return (start_date, parse_date_or_today(data_fim).date().isoformat())


@task(retries=1, retry_delay_seconds=30)
def query_api_page(
  secrets: Dict[str, str],
  data_inicio: str,
  data_fim: str,
  skip: int,
  limit: int = 100,
) -> Tuple[List[dict], int]:
  """Busca uma página de dados da API. Retorna (dados_da_página, total)."""

  # Resposta da API no formato:
  # {
  #   'total': xx,
  #   'skip': xx,
  #   'limit': xx,
  #   'data_inicio': 'xx',
  #   'data_fim': 'xx',
  #   'recurso_id': 'xx',
  #   'recurso_nome': 'xx',
  #   'validado_filtro': xx,
  #   'dados': [ ... ]
  # }
  resp = requests.get(
    secrets["url"],
    params={
      "recurso_id": secrets["resource"],
      "data_inicio": f"{data_inicio} 00:00:00-03:00",
      "data_fim": f"{data_fim} 00:00:00-03:00",
      "limit": limit,
      "skip": skip,
    },
    headers={"X-API-Key": secrets["key"]},
  )
  log(f"(query_api_page) API respondeu com status {resp.status_code} (skip={skip})")
  try:
    resp.raise_for_status()
  except Exception as e:
    log(resp.json(), level="error")
    raise e

  resp.encoding = "utf-8"
  json_resp = resp.json()

  page_data = json_resp["dados"]
  total = json_resp["total"]
  resource_name = json_resp["recurso_nome"]

  log(f"(query_api_page) Recebido(s) {len(page_data)} registro(s) (skip={skip}, total={total}, recurso='{resource_name}')")

  return page_data, total


@task
def upload_data(dataset_id: str, secrets: Dict[str, str], data: List[dict]):
  if len(data) <= 0:
    log("Dados vazios! Ignorando upload", level="warning")
    return

  # `data` é um arrao de dados no formato:
  # {
  #   'id': 'xx',
  #   'tipo_recurso_id': 'xx',
  #   'validado': xx,
  #   'erros_validacao': ...,
  #   'recebido_em': 'xx'
  #   'dados': {
  #     '_id': 'xx',
  #     '_updated_at': 'xx',
  #     ...
  #   },
  # }

  # Transforma campo `dados` em uma string JSON
  for i, d in enumerate(data):
    data[i]["dados"] = json.dumps(d["dados"])

  # Converte para DataFrame
  df = DataFrame(data)
  df["_extracted_at"] = now_naive()

  upload_df_to_datalake(
    df=df,
    dataset_id=dataset_id,
    table_id=secrets["table_id"],
    date_partition_column="_extracted_at",
    source_format="parquet",
    dump_mode="append",
  )
