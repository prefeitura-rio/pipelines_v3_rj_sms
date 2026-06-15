# -*- coding: utf-8 -*-
import json
from typing import Any

import google.auth.transport.requests
import google.oauth2.id_token
import requests
from google.cloud import storage

from pipelines.utils.logger import log


def cloud_function_request(
  cloud_function_url: str,
  url: str,
  credential: dict | None = None,
  request_type: str = "GET",
  body_params: Any = None,
  query_params: dict = None,
  header_params: dict = None,
  api_type: str = "json",
  endpoint_for_filename: str = None,
  timeout: int = 90,
) -> dict:
  request = google.auth.transport.requests.Request()
  token = google.oauth2.id_token.fetch_id_token(request, cloud_function_url)

  query_params_for_cf = dict(query_params or {})
  if endpoint_for_filename:
    query_params_for_cf["_endpoint_for_filename"] = endpoint_for_filename

  payload = {
    "tipo_api": api_type,
    "url": url,
    "request_type": request_type,
    "body_params": body_params,
    "query_params": query_params_for_cf,
    "header_params": header_params,
    "credential": credential,
  }
  if isinstance(body_params, dict) and api_type == "json":
    payload["body_params"] = json.dumps(body_params)

  headers = {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
  response = requests.post(
    cloud_function_url, headers=headers, data=json.dumps(payload), timeout=timeout
  )
  if response.status_code != 200:
    raise RuntimeError(
      f"[Cloud Function] Request failed: {response.status_code} - {response.reason}"
    )

  log("[Cloud Function] Request was successful")
  response_payload = response.json()
  if "gcs_url" in response_payload:
    gcs_url = response_payload["gcs_url"]
    log(f"[Cloud Function] GCS URL received. Downloading from: {gcs_url}")
    try:
      response_payload["body"] = download_cloud_function_body(
        gcs_url=gcs_url, api_type=api_type
      )
    except Exception as exc:
      message = f"[Cloud Function] Failed to download data from GCS ({gcs_url}): {exc}"
      log(message, level="error")
      raise RuntimeError(message) from exc

  if response_payload["status_code"] != 200:
    log(
      "[Target Endpoint] Request failed: "
      f"{response_payload['status_code']} - {response_payload['body']}",
      level="error",
    )
  else:
    log("[Target Endpoint] Request was successful")

  return response_payload


def download_cloud_function_body(gcs_url: str, api_type: str) -> Any:
  path_parts = gcs_url.replace("gs://", "").split("/", 1)
  if len(path_parts) < 2:
    raise ValueError(f"Invalid GCS URL format: {gcs_url}")

  bucket_name, blob_name = path_parts
  client = storage.Client()
  blob = client.bucket(bucket_name).blob(blob_name)
  downloaded_content = blob.download_as_text()
  if api_type == "json":
    return json.loads(downloaded_content)
  return downloaded_content
