# -*- coding: utf-8 -*-
import html
import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from pipelines.datalake.extract_load.vitacare_api_v2.constants import (
  constants as flow_constants,
)
from pipelines.utils.google_cloud import cloud_function_request
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_email
from pipelines.utils.prefect import authenticated_task as task

SAO_PAULO_TZ = ZoneInfo("America/Sao_Paulo")


def _format_target_date(target_date: date | datetime | str) -> str:
  if isinstance(target_date, str):
    return target_date
  return target_date.strftime("%Y-%m-%d")


@task
def generate_endpoint_params(
  estabelecimentos: pd.DataFrame,
  target_date: date | datetime | str,
  username: str,
  password: str,
  table_id_prefix: str | None = None,
) -> tuple[list[dict], list[str]]:
  """Gera parâmetros de extração agrupados por Área Programática."""
  estabelecimentos = estabelecimentos[
    ["id_cnes", "area_programatica", "prontuario_versao"]
  ].copy()
  estabelecimentos = estabelecimentos[estabelecimentos["prontuario_versao"] == "vitacare"]
  estabelecimentos = estabelecimentos.groupby("area_programatica").agg(
    cnes_list=("id_cnes", list)
  )

  params = []
  table_names = []
  target_date_str = _format_target_date(target_date)

  for ap, row in estabelecimentos.iterrows():
    params.append(
      {
        "ap": f"AP{ap}",
        "cnes_list": row["cnes_list"],
        "target_date": target_date_str,
        "username": username,
        "password": password,
      }
    )
    table_names.append(f"{table_id_prefix}_ap{ap}")

  return params, table_names


def _error_matches_timeout(error: Exception) -> bool:
  if isinstance(error, requests.exceptions.ReadTimeout):
    return True

  text = str(error).lower()
  return "timeout" in text or "read timed out" in text


@task
def extract_data(
  endpoint_params: dict, endpoint_name: str, environment: str = "dev", timeout: int = 90
) -> dict:
  if endpoint_name not in flow_constants.ENDPOINT.value:
    allowed = ", ".join(flow_constants.ENDPOINT.value.keys())
    raise ValueError(f"Endpoint '{endpoint_name}' inválido. Opções: {allowed}")

  log(
    f"Extracting data from API: {endpoint_params['ap']} {endpoint_name}. "
    f"There are {len(endpoint_params['cnes_list'])} CNES to extract."
  )

  base_url = get_secret(
    secret_name="API_URL_JSON",
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  base_url = json.loads(base_url)
  api_url = base_url[endpoint_params["ap"]] + flow_constants.ENDPOINT.value[endpoint_name]
  loaded_at = datetime.now(tz=SAO_PAULO_TZ)

  extraction_logs = []
  extracted_data = []

  for cnes in endpoint_params["cnes_list"]:
    current_datetime = datetime.now(tz=SAO_PAULO_TZ).strftime("%d/%m/%Y %H:%M:%S")
    log(
      "Extracting data from API: "
      f"({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
    )

    try:
      response = cloud_function_request.fn(
        url=api_url,
        endpoint_for_filename=endpoint_name,
        request_type="GET",
        query_params={"date": str(endpoint_params["target_date"]), "cnes": cnes},
        credential={
          "username": endpoint_params["username"],
          "password": endpoint_params["password"],
        },
        env=environment,
        timeout=timeout,
      )
    except Exception as error:
      is_timeout = _error_matches_timeout(error)
      result = (
        f"Timeout after {timeout} seconds" if is_timeout else f"Unexpected error: {error}"
      )
      extraction_logs.append(
        {
          "ap": endpoint_params["ap"],
          "cnes": cnes,
          "target_date": endpoint_params["target_date"],
          "endpoint_name": endpoint_name,
          "endpoint_url": api_url,
          "datetime": current_datetime,
          "success": False,
          "result": result,
        }
      )
      log(
        (
          "Timeout extracting data from API: "
          if is_timeout
          else "Error extracting data from API: "
        )
        + f"({cnes}, {endpoint_params['target_date']}, {endpoint_name}) {error}",
        level="error" if not is_timeout else "warning",
      )
      continue

    if response["status_code"] != 200:
      extraction_logs.append(
        {
          "ap": endpoint_params["ap"],
          "cnes": cnes,
          "target_date": endpoint_params["target_date"],
          "endpoint_name": endpoint_name,
          "endpoint_url": api_url,
          "datetime": current_datetime,
          "success": False,
          "result": f"Status Code {response['status_code']}: {response['body']}",
        }
      )
      log(
        "Error extracting data from API: "
        f"({cnes}, {endpoint_params['target_date']}, {endpoint_name}) "
        f"{response['status_code']}",
        level="error",
      )
      continue

    extraction_logs.append(
      {
        "ap": endpoint_params["ap"],
        "cnes": cnes,
        "target_date": endpoint_params["target_date"],
        "endpoint_name": endpoint_name,
        "endpoint_url": api_url,
        "datetime": current_datetime,
        "success": True,
        "result": f"Status Code {response['status_code']}",
      }
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

  if extracted_data:
    return {"data": pd.concat(extracted_data), "logs": extraction_logs}

  return {"data": pd.DataFrame(), "logs": extraction_logs}


def _calculate_metrics(logs_df: pd.DataFrame) -> tuple[float, float, float, float]:
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


def _paragraph(text: str) -> str:
  return f"<p>{html.escape(text)}</p>"


@task
def send_email_notification(
  logs: list[list[dict]], endpoint: str, environment: str, target_date: str
) -> None:
  logs = [log_item for sublist in logs for log_item in sublist]
  logs_df = pd.DataFrame(logs)
  if not logs_df.empty:
    logs_df.sort_values(by=["ap", "datetime"], inplace=True)

  success_rate, delay_rate, error_rate, other_error_rate = _calculate_metrics(logs_df)

  parts = [f"<h2>Extração de <code>{html.escape(endpoint)}</code></h2>"]
  parts.append(_paragraph(f"Data alvo: {target_date}"))
  parts.append("<ul>")
  parts.append(f"<li>Taxa Geral de Sucesso: {success_rate:.2f}%</li>")
  parts.append(f"<li>Taxa Geral de Atraso de Replicação: {delay_rate:.2f}%</li>")
  parts.append(f"<li>Taxa Geral de Indisponibilidade: {error_rate:.2f}%</li>")
  parts.append(f"<li>Taxa Geral de Outros Erros: {other_error_rate:.2f}%</li>")
  parts.append("</ul>")

  parts.append("<h3>Por Área Programática</h3>")
  parts.append("<ul>")
  if not logs_df.empty:
    for ap, ap_logs in logs_df.groupby("ap"):
      success_rate, delay_rate, error_rate, other_error_rate = _calculate_metrics(ap_logs)
      parts.append(
        f"<li><strong>{html.escape(str(ap))}</strong> - "
        f"{success_rate:.2f}% | {delay_rate:.2f}% | "
        f"{error_rate:.2f}% | {other_error_rate:.2f}%</li>"
      )
  parts.append("</ul>")

  parts.append("<h3>Por Estabelecimento</h3>")
  parts.append("<ul>")
  if not logs_df.empty:
    for _, row in logs_df.iterrows():
      if not row["success"]:
        parts.append(
          f"<li>[{html.escape(str(row['ap']))}] "
          f"CNES: {html.escape(str(row['cnes']))} "
          f"às {html.escape(str(row['datetime']))}: "
          f"<code>{html.escape(str(row['result']))}</code></li>"
        )
  parts.append("</ul>")

  target_emails = get_secret(
    secret_name="REPORT_TARGET_EMAILS",
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  target_emails = json.loads(target_emails)

  send_email(
    subject=f"Resultados de Extração - Endpoint {endpoint}",
    message="\n".join(parts),
    recipients={"to_addresses": target_emails, "cc_addresses": [], "bcc_addresses": []},
    environment=environment,
  )
