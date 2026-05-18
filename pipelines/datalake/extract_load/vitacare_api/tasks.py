# -*- coding: utf-8 -*-
import json

import pandas as pd
import requests
from markdown_it import MarkdownIt

from pipelines.utils.api import cloud_function_request
from pipelines.utils.datetime import now
from pipelines.utils.google import load_file_from_bigquery
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_email
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants as flow_constants


@task
def generate_endpoint_params(
  target_date, environment: str = "dev", table_id_prefix: str = None
) -> tuple[list[dict], list[str]]:
  username = get_secret(
    secret_name=flow_constants.INFISICAL_VITACARE_USERNAME.value,
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  password = get_secret(
    secret_name=flow_constants.INFISICAL_VITACARE_PASSWORD.value,
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  estabelecimentos = load_file_from_bigquery(
    project_name="rj-sms",
    dataset_name="saude_dados_mestres",
    table_name="estabelecimento",
  )[["id_cnes", "area_programatica", "prontuario_versao"]]
  estabelecimentos = estabelecimentos[
    estabelecimentos["prontuario_versao"] == "vitacare"
  ]
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


@task
def get_property_from_dict(data: dict, key: str):
  return data[key]


@task
def extract_data(
  endpoint_params: dict, endpoint_name: str, environment: str = "dev", timeout: int = 90
) -> dict:
  log(
    f"Extracting data from API: {endpoint_params['ap']} {endpoint_name}."
    + f" There are {len(endpoint_params['cnes_list'])} CNES to extract."
  )
  base_url = get_secret(
    secret_name="API_URL_JSON",
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  base_url = json.loads(base_url)
  api_url = base_url[endpoint_params["ap"]] + flow_constants.ENDPOINT.value[endpoint_name]
  loaded_at = now()

  extraction_logs = []
  extracted_data = []
  for cnes in endpoint_params["cnes_list"]:
    current_datetime = now().strftime("%d/%m/%Y %H:%M:%S")
    log(
      "Extracting data from API:"
      + f" ({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
    )
    try:
      response = cloud_function_request(
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
    except requests.exceptions.Timeout:
      extraction_logs.append(
        {
          "ap": endpoint_params["ap"],
          "cnes": cnes,
          "target_date": endpoint_params["target_date"],
          "endpoint_name": endpoint_name,
          "endpoint_url": api_url,
          "datetime": current_datetime,
          "success": False,
          "result": f"Timeout after {timeout} seconds",
        }
      )
      log(
        "Timeout extracting data from API:"
        + f" ({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
      )
      continue
    except Exception as e:
      extraction_logs.append(
        {
          "ap": endpoint_params["ap"],
          "cnes": cnes,
          "target_date": endpoint_params["target_date"],
          "endpoint_name": endpoint_name,
          "endpoint_url": api_url,
          "datetime": current_datetime,
          "success": False,
          "result": f"Unexpected error: {e}",
        }
      )
      log(
        "Error extracting data from API:"
        + f" ({cnes}, {endpoint_params['target_date']}, {endpoint_name}) {e}"
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
        "Error extracting data from API:"
        + f" ({cnes}, {endpoint_params['target_date']}, {endpoint_name})"
        + f" {response['status_code']}"
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
    rows = [json.dumps(x) for x in response["body"]]
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

  if len(extracted_data) > 0:
    return {"data": pd.concat(extracted_data), "logs": extraction_logs}
  return {"data": pd.DataFrame(), "logs": extraction_logs}


@task
def send_email_notification(logs: list, endpoint: str, environment: str, target_date):
  logs = [log for sublist in logs for log in sublist]
  logs_df = pd.DataFrame(logs)
  logs_df.sort_values(by=["ap", "datetime"], inplace=True)

  def calculate_metrics(dataframe: pd.DataFrame):
    if dataframe.shape[0] == 0:
      return 0, 0, 0, 0

    success = dataframe["success"].astype(bool)
    result_str = dataframe["result"].astype(str)
    total = float(len(dataframe))
    delayed_mask = (~success) & (result_str.str.contains("404", na=False))
    error_mask = (~success) & (result_str.str.contains("503", na=False))
    success_rate = success.sum() / total
    delay_rate = delayed_mask.sum() / total
    error_rate = error_mask.sum() / total
    other_error_rate = max(0.0, 1.0 - success_rate - delay_rate - error_rate)
    return success_rate * 100, delay_rate * 100, error_rate * 100, other_error_rate * 100

  success_rate, delay_rate, error_rate, other_error_rate = calculate_metrics(logs_df)

  message = f"## Extração de `{endpoint}`\n"
  message += f"- Taxa Geral de Sucesso: {success_rate:.2f}% \n"
  message += f"- Taxa Geral de Atraso de Replicação: {delay_rate:.2f}% \n"
  message += f"- Taxa Geral de Indisponibilidade: {error_rate:.2f}% \n"
  message += f"- Taxa Geral de Outros Erros: {other_error_rate:.2f}% \n"

  message += "### Por Área Programática\n"
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
        f"- [{row['ap']}] CNES: {row['cnes']} às {row['datetime']}: "
        f"`{row['result']}`\n"
      )

  target_emails = get_secret(
    secret_name="REPORT_TARGET_EMAILS",
    path=flow_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  target_emails = json.loads(target_emails)

  send_email(
    subject=f"Resultados de Extração - Endpoint {endpoint}",
    message=MarkdownIt().render(message),
    recipients={"to_addresses": target_emails, "cc_addresses": [], "bcc_addresses": []},
    environment=environment,
  )
