# -*- coding: utf-8 -*-
import json
from typing import Any

import requests

from pipelines.utils.infisical import get_secret


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


def send_email(
  subject: str,
  message: str,
  recipients: dict,
  environment: str,
  is_html_body: bool = True,
  secret_path: str = "/datarelay",
  endpoint: str = "/data/mailman",
) -> None:
  url_secret = get_secret(
    secret_name="API_URL", path=secret_path, environment=environment
  )
  token_secret = get_secret(
    secret_name="API_TOKEN", path=secret_path, environment=environment
  )

  url = _coerce_secret_value(url_secret, "API_URL").rstrip("/?#")
  token = _coerce_secret_value(token_secret, "API_TOKEN")

  response = requests.post(
    f"{url}{endpoint}",
    headers={"x-api-key": token},
    json={
      **recipients,
      "subject": subject,
      "body": message,
      "is_html_body": is_html_body,
    },
  )
  response.raise_for_status()
  response.encoding = response.apparent_encoding
  response_json = response.json()

  if response_json.get("success"):
    return

  raise RuntimeError(f"Email delivery failed: {response_json}")
