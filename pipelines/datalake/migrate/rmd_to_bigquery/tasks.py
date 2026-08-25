# -*- coding: utf-8 -*-
import base64
import binascii
import json
from typing import Dict, List, Tuple

import requests
from google.cloud import storage
from pandas import DataFrame

from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.datetime import (
  from_relative_date,
  now_naive,
  parse_date_or_today,
  today_str,
)
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import FORNECEDORES_COM_LAUDO_BASE64, RecursosRMD, resource_to_table_map


@task
def get_secrets(recurso: RecursosRMD, environment: str):
  api_url = get_secret(secret_name="API_URL", path="/rmd", environment=environment)
  consumer_endpoint = get_secret(
    secret_name="CONSUMER_ENDPOINT", path="/rmd", environment=environment
  )
  api_full_url = f"{api_url}{consumer_endpoint}"

  resource_id = get_secret(secret_name=recurso, path="/rmd", environment=environment)

  consumer_key = get_secret(
    secret_name="CONSUMER_KEY", path="/rmd", environment=environment
  )
  return {
    "url": api_full_url,
    "resource": resource_id,
    "key": consumer_key,
    "table_id": resource_to_table_map[recurso],
  }


@task
def calculate_date_interval(data_inicio: str, data_fim: str | None) -> Tuple[str, str]:
  start_date = from_relative_date(data_inicio).isoformat()

  if not data_fim:
    return (start_date, today_str())

  return (start_date, parse_date_or_today(data_fim).date().isoformat())


@task(retries=1, retry_delay_seconds=30, persist_result=False)
def query_api_page(
  secrets: Dict[str, str], data_inicio: str, data_fim: str, skip: int, limit: int = 100
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

  log(
    f"(query_api_page) Recebido(s) {len(page_data)} registro(s) (skip={skip}, total={total}, recurso='{resource_name}')"
  )

  return page_data, total


def _upload_laudo_to_gcs(record_id: str, recebido_em: str, b64_string: str, bucket_name: str) -> str:
  """Decodifica o laudo PDF em base64 e faz upload para o GCS. Retorna o URI gs://..."""
  try:
    pdf_bytes = base64.b64decode(b64_string)
  except binascii.Error:
    log(
      f"(_upload_laudo_to_gcs) Valor de 'exame_resultado_laudo' para o registro '{record_id}' não é base64 válido; mantendo valor original."
    )
    return b64_string
  recebido_em_clean = recebido_em[:19]  # "2026-08-25T18:12:07.877078+00:00" -> "2026-08-25T18:12:07"
  prefix = f"{recebido_em_clean}_" if recebido_em_clean else ""
  blob_name = f"staging/brutos_rmd_laudos/{prefix}{record_id}.pdf"
  blob = storage.Client().bucket(bucket_name).blob(blob_name)
  blob.upload_from_string(pdf_bytes, content_type="application/pdf")
  gcs_uri = f"gs://{bucket_name}/{blob_name}"
  log(f"(_upload_laudo_to_gcs) PDF enviado para '{gcs_uri}'")
  return gcs_uri


@task(persist_result=False)
def upload_data(
  dataset_id: str, secrets: Dict[str, str], data: List[dict], environment: str
):
  if len(data) <= 0:
    log("Dados vazios! Ignorando upload", level="warning")
    return

  # `data` é um array de dados no formato:
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

  # Para fornecedores que enviam o laudo como base64 (ex: Blessing), extrai o PDF,
  # faz upload para o GCS e substitui o campo pelo URI gs://.
  # Os demais fornecedores enviam o laudo via endpoint separado e não precisam desse tratamento.
  gcs_bucket = None
  for d in data:
    if d.get("fornecedor_id") in FORNECEDORES_COM_LAUDO_BASE64:
      laudo = d.get("dados", {}).get("exame_resultado_laudo")
      if laudo:
        if gcs_bucket is None:
          gcs_bucket = get_google_project_for_environment(environment)
        d["dados"]["exame_resultado_laudo"] = _upload_laudo_to_gcs(
          record_id=d["id"],
          recebido_em=d.get("recebido_em", ""),
          b64_string=laudo,
          bucket_name=gcs_bucket,
        )

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
