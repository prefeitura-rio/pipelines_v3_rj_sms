# -*- coding: utf-8 -*-
import json
from typing import Any

import requests
from markdown_it import MarkdownIt

from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log


def _coerce_secret_value(secret_value: Any, key: str) -> str:
  if isinstance(secret_value, dict):
    return secret_value[key]

  if isinstance(secret_value, str):
    try:
      parsed = json.loads(secret_value)
      if isinstance(parsed, dict) and key in parsed:
        return parsed[key]
    except json.JSONDecodeError:
      pass
    return secret_value

  return str(secret_value)


def send_email(subject: str, message: str, recipients: dict, environment: str):
  url_secret = get_secret(
    secret_name="API_URL", path="/datarelay", environment=environment
  )
  token_secret = get_secret(
    secret_name="API_TOKEN", path="/datarelay", environment=environment
  )

  url = _coerce_secret_value(url_secret, "API_URL").rstrip("/?#")
  token = _coerce_secret_value(token_secret, "API_TOKEN")

  response = requests.post(
    f"{url}/data/mailman",
    headers={"x-api-key": token},
    json={**recipients, "subject": subject, "body": message, "is_html_body": True},
  )
  response.raise_for_status()
  response.encoding = response.apparent_encoding
  response_json = response.json()

  if response_json.get("success"):
    return

  raise RuntimeError(f"Email delivery failed: {response_json}")


def send_api_error_report(status_code: int, source: str, environment: str):
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
