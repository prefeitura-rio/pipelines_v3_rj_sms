# -*- coding: utf-8 -*-
import json
from typing import Any

import pandas as pd
import requests
from google.cloud import bigquery
from markdown_it import MarkdownIt

from pipelines.datalake.extract_load.vitacare_api.constants import (
  constants as vitacare_constants,
)
from pipelines.utils.cloud_function import cloud_function_request
from pipelines.utils.datetime import now, now_str
from pipelines.utils.email import send_email
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def generate_endpoint_params(
  target_date: Any, environment: str = "dev", table_id_prefix: str = None
) -> tuple[list[dict], list[str]]:
  username = get_secret(
    path=vitacare_constants.INFISICAL_PATH.value,
    secret_name=vitacare_constants.INFISICAL_VITACARE_USERNAME.value,
    environment=environment,
  )
  password = get_secret(
    path=vitacare_constants.INFISICAL_PATH.value,
    secret_name=vitacare_constants.INFISICAL_VITACARE_PASSWORD.value,
    environment=environment,
  )

  estabelecimentos = load_estabelecimentos()
  estabelecimentos = estabelecimentos[estabelecimentos["prontuario_versao"] == "vitacare"]
  estabelecimentos = estabelecimentos.groupby("area_programatica").agg(
    cnes_list=("id_cnes", list)
  )

  params = []
  table_names = []
  for ap, df in estabelecimentos.iterrows():
    params.append(
      {
        "ap": f"AP{ap}",
        "cnes_list": df["cnes_list"],
        "target_date": target_date.strftime("%Y-%m-%d"),
        "username": username,
        "password": password,
      }
    )
    table_names.append(f"{table_id_prefix}_ap{ap}")

  return params, table_names


def load_estabelecimentos() -> pd.DataFrame:
  client = bigquery.Client()
  table_id = (
    f"{vitacare_constants.ESTABELECIMENTO_PROJECT.value}."
    f"{vitacare_constants.ESTABELECIMENTO_DATASET.value}."
    f"{vitacare_constants.ESTABELECIMENTO_TABLE.value}"
  )
  log(f"(load_estabelecimentos) carregando `{table_id}`")
  table = client.get_table(table_id)
  return client.list_rows(table).to_dataframe()[
    ["id_cnes", "area_programatica", "prontuario_versao"]
  ]


@task
def extract_data(
  endpoint_params: dict, endpoint_name: str, environment: str = "dev", timeout: int = 90
) -> dict:
  log(
    f"(extract_data) extraindo {endpoint_params['ap']} {endpoint_name}; "
    f"{len(endpoint_params['cnes_list'])} CNES"
  )
  base_url = get_secret(
    path=vitacare_constants.INFISICAL_PATH.value,
    secret_name="API_URL_JSON",
    environment=environment,
  )
  base_url = json.loads(base_url)
  api_url = (
    base_url[endpoint_params["ap"]] + vitacare_constants.ENDPOINT.value[endpoint_name]
  )

  extraction_logs, extracted_data = [], []
  loaded_at = now()
  for cnes in endpoint_params["cnes_list"]:
    current_datetime = now_str()
    log(
      "(extract_data) requisitando "
      f"({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
    )
    try:
      response = cloud_function_request(
        cloud_function_url=vitacare_constants.CLOUD_FUNCTION_URL.value,
        url=api_url,
        endpoint_for_filename=endpoint_name,
        request_type="GET",
        query_params={"date": str(endpoint_params["target_date"]), "cnes": cnes},
        credential={
          "username": endpoint_params["username"],
          "password": endpoint_params["password"],
        },
        timeout=timeout,
      )
    except requests.exceptions.ReadTimeout:
      extraction_logs.append(
        build_extraction_log(
          endpoint_params=endpoint_params,
          endpoint_name=endpoint_name,
          endpoint_url=api_url,
          cnes=cnes,
          datetime=current_datetime,
          success=False,
          result=f"Timeout after {timeout} seconds",
        )
      )
      log(
        "(extract_data) timeout na API: "
        f"({cnes}, {endpoint_params['target_date']}, {endpoint_name})",
        level="warning",
      )
      continue
    except Exception as exc:
      extraction_logs.append(
        build_extraction_log(
          endpoint_params=endpoint_params,
          endpoint_name=endpoint_name,
          endpoint_url=api_url,
          cnes=cnes,
          datetime=current_datetime,
          success=False,
          result=f"Unexpected error: {exc}",
        )
      )
      log(
        "(extract_data) erro na API: "
        f"({cnes}, {endpoint_params['target_date']}, {endpoint_name}) {exc}",
        level="error",
      )
      continue

    if response["status_code"] != 200:
      extraction_logs.append(
        build_extraction_log(
          endpoint_params=endpoint_params,
          endpoint_name=endpoint_name,
          endpoint_url=api_url,
          cnes=cnes,
          datetime=current_datetime,
          success=False,
          result=f"Status Code {response['status_code']}: {response['body']}",
        )
      )
      log(
        "(extract_data) resposta da API com erro: "
        f"({cnes}, {endpoint_params['target_date']}, {endpoint_name}) "
        f"{response['status_code']}",
        level="error",
      )
      continue

    extraction_logs.append(
      build_extraction_log(
        endpoint_params=endpoint_params,
        endpoint_name=endpoint_name,
        endpoint_url=api_url,
        cnes=cnes,
        datetime=current_datetime,
        success=True,
        result=f"Status Code {response['status_code']}",
      )
    )

    rows = [json.dumps(row) for row in response["body"]]
    requested_data = pd.DataFrame(
      {
        "data": rows,
        "_source_cnes": cnes,
        "_source_ap": endpoint_params["ap"],
        "_target_date": endpoint_params["target_date"],
        "_endpoint": endpoint_name,
        "_loaded_at": loaded_at,
      }
    )
    extracted_data.append(requested_data)

  data = pd.concat(extracted_data) if extracted_data else pd.DataFrame()
  return {"data": data, "logs": extraction_logs}


def build_extraction_log(
  endpoint_params: dict,
  endpoint_name: str,
  endpoint_url: str,
  cnes: str,
  datetime: str,
  success: bool,
  result: str,
) -> dict:
  return {
    "ap": endpoint_params["ap"],
    "cnes": cnes,
    "target_date": endpoint_params["target_date"],
    "endpoint_name": endpoint_name,
    "endpoint_url": endpoint_url,
    "datetime": datetime,
    "success": success,
    "result": result,
  }


@task
def send_email_notification(
  logs: list, endpoint: str, environment: str, target_date: Any
) -> None:
  flattened_logs = [log_item for sublist in logs for log_item in sublist]
  logs_df = pd.DataFrame(flattened_logs)
  if logs_df.empty:
    message = f"## Extracao de `{endpoint}`\nNenhuma requisicao foi registrada."
  else:
    logs_df.sort_values(by=["ap", "datetime"], inplace=True)
    message = build_email_message(logs_df=logs_df, endpoint=endpoint)

  target_emails = get_secret(
    path=vitacare_constants.INFISICAL_PATH.value,
    secret_name="REPORT_TARGET_EMAILS",
    environment=environment,
  )
  target_emails = json.loads(target_emails)
  send_email(
    subject=f"Resultados de Extracao - Endpoint {endpoint} - {target_date}",
    message=MarkdownIt().render(message),
    recipients={"to_addresses": target_emails, "cc_addresses": [], "bcc_addresses": []},
    environment=environment,
  )


def build_email_message(logs_df: pd.DataFrame, endpoint: str) -> str:
  success_rate, delay_rate, error_rate, other_error_rate = calculate_metrics(logs_df)

  message = f"## Extracao de `{endpoint}`\n"
  message += f"- Taxa Geral de Sucesso: {success_rate:.2f}% \n"
  message += f"- Taxa Geral de Atraso de Replicacao: {delay_rate:.2f}% \n"
  message += f"- Taxa Geral de Indisponibilidade: {error_rate:.2f}% \n"
  message += f"- Taxa Geral de Outros Erros: {other_error_rate:.2f}% \n"

  message += "### Por Area Programatica\n"
  for ap, ap_logs in logs_df.groupby("ap"):
    success_rate, delay_rate, error_rate, other_error_rate = calculate_metrics(ap_logs)
    message += (
      f"- **{ap}** - {success_rate:.2f}% | {delay_rate:.2f}% | "
      f"{error_rate:.2f}% | {other_error_rate:.2f}%\n"
    )

  message += "### Por Estabelecimento\n"
  for _, row in logs_df.iterrows():
    if not row["success"]:
      message += (
        f"- [{row['ap']}] CNES: {row['cnes']} as {row['datetime']}: `{row['result']}`\n"
      )

  return message


def calculate_metrics(logs_df: pd.DataFrame) -> tuple[float, float, float, float]:
  if logs_df.shape[0] == 0:
    return 0, 0, 0, 0

  success = logs_df["success"].astype(bool)
  result_str = logs_df["result"].astype(str)
  total = float(len(logs_df))

  delayed_mask = (~success) & (result_str.str.contains("404", na=False))
  error_mask = (~success) & (result_str.str.contains("503", na=False))

  success_rate = success.sum() / total
  delay_rate = delayed_mask.sum() / total
  error_rate = error_mask.sum() / total
  other_error_rate = max(0.0, 1.0 - success_rate - delay_rate - error_rate)

  return success_rate * 100, delay_rate * 100, error_rate * 100, other_error_rate * 100
