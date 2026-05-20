# -*- coding: utf-8 -*-
import json
import uuid
from datetime import timedelta

import pandas as pd
import requests
from markdown_it import MarkdownIt

from pipelines.datalake.extract_load.exames_laboratoriais.constants import (
  AREA_PROGRAMATICA,
  CREDENTIALS,
)
from pipelines.utils.datetime import now_str
from pipelines.utils.email import send_email
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


def _authenticate_fetch(
  username: str,
  apccodigo: str,
  password: str,
  identificador_lis: str,
  dt_start: str,
  dt_end: str,
  environment: str,
  source: str,
  base_url: str,
) -> dict:
  auth_headers = {"emissor": username, "apccodigo": apccodigo, "pass": password}
  base_url = base_url.rstrip("/")

  try:
    token_response = requests.get(
      f"{base_url}/tokenlisnet/apccodigo", headers=auth_headers
    )

    token_data = token_response.json()

    if token_data.get("status") != 200:
      message = (
        f"(authenticate_and_fetch) Error getting token: {token_data.get('mensagem')}"
      )
      raise Exception(message)

    token = token_data["token"]
    log("Authentication was successful")

    results_headers = {"codigo": apccodigo, "token": token}
    request_body = {
      "lote": {
        "identificadorLis": identificador_lis,
        "dataResultado": {"inicial": dt_start, "final": dt_end},
        "parametros": {"retorno": "ESTRUTURADO/LINK", "parcial": "N", "sigiloso": "S"},
      }
    }

    results_response = requests.post(
      f"{base_url}/APOIO/DTL/resultado", headers=results_headers, json=request_body
    )

    if results_response.status_code in [502, 503]:
      message = (
        f"(authenticate_fetch) Service Unavailable (Status {results_response.status_code}). "
        "Possível manutenção ou instabilidade na API"
      )
      log(message, level="warning")

      send_api_error_report(
        status_code=results_response.status_code, source=source, environment=environment
      )

      return {
        "lote": {"status": results_response.status_code, "mensagem": "API Fora do Ar"}
      }

    results = results_response.json()

    if isinstance(results, str):
      error_message = f"(authenticate_fetch) request failed: {results}"
      log(error_message, level="error")
      raise Exception(error_message)

    if "lote" in results and results["lote"].get("status") != 200:
      lote_status = results["lote"].get("status")
      lote_mensagem = results["lote"].get("mensagem")

      if (
        lote_status == 501
        and "Resultado não disponíveis para data solicitada" in lote_mensagem
      ):
        log(f"(authenticate_fetch) Status 501: {lote_mensagem}", level="warning")
        return results

      if lote_status is not None and lote_status != 200:
        message = (
          "(authenticate_and_fetch) Failed to get results: "
          f"Status: {lote_status} Message: {lote_mensagem}"
        )
        raise Exception(message)

    return results

  except Exception as exc:
    error_message = str(exc)
    log(f"(authenticate_and_fetch) Unexpected error: {error_message}", level="error")
    raise


def _transform(json_result: dict, source: str):
  solicitacoes_rows = []
  exames_rows = []
  resultados_rows = []

  lote = json_result.get("lote")

  if not lote:
    message = "(transform) lote not found in json response"
    raise ValueError(message)

  solicitacoes = lote.get("solicitacoes", {}).get("solicitacao", [])

  if isinstance(solicitacoes, dict):
    solicitacoes = [solicitacoes]

  lote_attrs = {f"lote_{k}": v for k, v in lote.items() if not isinstance(v, dict)}

  for solicitacao in solicitacoes:
    solicitacoes_row = {
      k: v for k, v in solicitacao.items() if not isinstance(v, (dict, list))
    }

    solicitacoes_row.update(lote_attrs)

    for entidade_suporte in ["responsaveltecnico", "paciente"]:
      nested_dict = solicitacao.get(entidade_suporte, {})
      for key, value in nested_dict.items():
        solicitacoes_row[f"{entidade_suporte}_{key}"] = value

    solicitacao_id = str(
      uuid.uuid5(
        uuid.NAMESPACE_DNS,
        f"{solicitacoes_row.get('codigoLis', '')}|"
        f"{solicitacoes_row.get('codigoApoio', '')}|"
        f"{solicitacoes_row.get('dataPedido', '')}|"
        f"{solicitacoes_row.get('paciente_nome', '')}|"
        f"{solicitacoes_row.get('codunidade', '')}|"
        f"{solicitacoes_row.get('origem', '')}|"
        f"{solicitacoes_row.get('paciente_cpf', '')}",
      )
    )
    solicitacoes_row["id"] = solicitacao_id
    solicitacoes_rows.append(solicitacoes_row)

    exames = solicitacao.get("exames", {}).get("exame", [])

    if isinstance(exames, dict):
      exames = [exames]

    for exame in exames:
      exames_row = {k: v for k, v in exame.items() if not isinstance(v, (dict, list))}
      exames_row["solicitacao_id"] = solicitacao_id

      solicitante_dict = exame.get("solicitante", {})
      for key, value in solicitante_dict.items():
        exames_row[f"solicitante_{key}"] = value

      exame_id = str(
        uuid.uuid5(
          uuid.NAMESPACE_DNS,
          f"{solicitacao_id}|{exames_row.get('codigoExame', '')}|"
          f"{exames_row.get('codigoApoio', '')}|{exames_row.get('dataAssinatura', '')}",
        )
      )
      exames_row["id"] = exame_id
      exames_rows.append(exames_row)

      resultados = exame.get("resultados", {}).get("resultado", [])

      if isinstance(resultados, dict):
        resultados = [resultados]

      for resultado in resultados:
        resultados_row = {k: v for k, v in resultado.items()}
        resultados_row["exame_id"] = exame_id

        resultado_id = str(
          uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{resultados_row.get('codigoApoio', '')}|"
            f"{resultados_row.get('descricaoApoio', '')}|"
            f"{exame_id}",
          )
        )
        resultados_row["id"] = resultado_id
        resultados_rows.append(resultados_row)

  loaded_at = now_str()

  solicitacoes_df = pd.DataFrame(solicitacoes_rows)
  exames_df = pd.DataFrame(exames_rows)
  resultados_df = pd.DataFrame(resultados_rows)

  for df in [solicitacoes_df, exames_df, resultados_df]:
    if not df.empty:
      df["datalake_loaded_at"] = loaded_at
      df["source"] = source

  return solicitacoes_df, exames_df, resultados_df


def _parse_identificador(identificador: str, ap: str) -> str:
  try:
    identificador_corrigido = identificador.replace("“", '"').replace("”", '"')
    identificador_dict = json.loads(identificador_corrigido)
    key = f"AP{ap}"
    identificador_lis = identificador_dict.get(key) or identificador_dict.get(ap)
    return identificador_lis
  except (json.JSONDecodeError, TypeError, AttributeError):
    return identificador


def _get_extraction_context(ap: str, environment: str) -> dict:
  source = AREA_PROGRAMATICA.get(ap)

  if not source:
    message = f"AP {ap} não encontrada no mapeamento AREA_PROGRAMATICA."
    log(message, level="error")
    raise ValueError(message)

  credential = CREDENTIALS[source]
  infisical_path = credential["INFISICAL_PATH"]

  username_secret = get_secret(
    secret_name=credential["INFISICAL_USERNAME"],
    path=infisical_path,
    environment=environment,
  )
  password_secret = get_secret(
    secret_name=credential["INFISICAL_PASSWORD"],
    path=infisical_path,
    environment=environment,
  )
  apccodigo_secret = get_secret(
    secret_name=credential["INFISICAL_APCCODIGO"],
    path=infisical_path,
    environment=environment,
  )
  identificador_lis_secret = get_secret(
    secret_name=credential["INFISICAL_AP_LIS"],
    path=infisical_path,
    environment=environment,
  )
  base_url_secret = get_secret(
    secret_name=credential["INFISICAL_BASE_URL"],
    path=infisical_path,
    environment=environment,
  )

  return {
    "source": source,
    "username": username_secret,
    "password": password_secret,
    "apccodigo": apccodigo_secret,
    "identificador_lis": _parse_identificador(
      identificador=identificador_lis_secret, ap=ap
    ),
    "base_url": base_url_secret,
  }


@task(retries=3, retry_delay_seconds=60)
def extract_exames_laboratoriais(
  ap: str,
  environment: str,
  dt_inicio: str,
  dt_fim: str,
  dataset: str,
) -> dict:
  context = _get_extraction_context(ap=ap, environment=environment)
  results = _authenticate_fetch(
    username=context["username"],
    password=context["password"],
    apccodigo=context["apccodigo"],
    identificador_lis=context["identificador_lis"],
    dt_start=dt_inicio,
    dt_end=dt_fim,
    environment=environment,
    source=context["source"],
    base_url=context["base_url"],
  )
  solicitacoes_df, exames_df, resultados_df = _transform(
    json_result=results, source=context["source"]
  )

  return {
    "ap": ap,
    "dt_inicio": dt_inicio,
    "dt_fim": dt_fim,
    "dataset": dataset,
    "source": context["source"],
    "solicitacoes_df": solicitacoes_df,
    "exames_df": exames_df,
    "resultados_df": resultados_df,
  }


@task
def get_all_aps():
  return list(AREA_PROGRAMATICA.keys())


@task
def generate_time_windows(
  start_date: pd.Timestamp, end_date: str, hours_per_window: int | float = 2
):
  """
  Gera janelas de tempo desde a start_date até a meia-noite do dia de referência.
  """
  tz = "America/Sao_Paulo"

  start_date = pd.Timestamp(start_date)
  if start_date.tzinfo is None:
    start_date = start_date.tz_localize(tz)

  if end_date:
    end_date_limit = pd.to_datetime(end_date)
    if end_date_limit.tzinfo is None:
      end_date_limit = end_date_limit.tz_localize(tz)
    end_date_limit = end_date_limit.normalize()
  else:
    end_date_limit = pd.Timestamp.now(tz).normalize()

  start_points = pd.date_range(
    start=start_date, end=end_date_limit, freq=f"{hours_per_window}h", inclusive="left"
  )

  windows = []
  for window_start in start_points:
    window_end = window_start + timedelta(hours=hours_per_window, seconds=-1)

    if window_end >= end_date_limit:
      continue

    windows.append(
      {
        "dt_inicio": window_start.strftime("%Y-%m-%d %H:%M:%S%z"),
        "dt_fim": window_end.strftime("%Y-%m-%d %H:%M:%S%z"),
      }
    )

  log(
    f"{len(windows)} janelas geradas com sucesso de {start_date.date()} até "
    f"{end_date_limit.date()}"
  )
  return windows


@task
def build_extraction_params(windows: list, aps: list, env: str, dataset: str):
  """
  Cria uma entrada de parâmetros para cada combinação de AP e janela de tempo.
  """
  params = []

  for window in windows:
    for ap in aps:
      params.append(
        {
          "dt_inicio": window["dt_inicio"],
          "dt_fim": window["dt_fim"],
          "ap": ap,
          "environment": env,
          "dataset": dataset,
        }
      )

  log(f"Parâmetros gerados para {len(params)} combinações (AP x Janela).")
  return params


def send_api_error_report(status_code: int, source: str, environment: str) -> None:
  md = MarkdownIt()

  subject = f"[ALERTA] API {source.upper()} Indisponível"
  message = "## A extração de exames laboratoriais encontrou um erro de servidor.\n\n"
  message += f"- **Sistema:** {source.upper()}\n"
  message += f"- **Status Code:** {status_code}\n"
  message += f"- **Ambiente:** {environment}\n"
  message += "- **Motivo:** Servidor em manutenção ou indisponível.\n\n"
  message += "---\n"
  message += "*O fluxo continuará tentando a extração automaticamente conforme a política de retries.*"

  recipients = ["daniel.lira@prefeitura.rio"]

  try:
    send_email(
      subject=subject,
      message=md.render(message),
      recipients={"to_addresses": recipients, "cc_addresses": [], "bcc_addresses": []},
      environment=environment,
    )
    log(f"E-mail de alerta enviado com sucesso para {recipients}")
  except Exception as exc:
    log(f"Falha ao enviar e-mail de alerta: {str(exc)}", level="error")
