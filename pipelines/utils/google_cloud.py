# -*- coding: utf-8 -*-
import json
from typing import Any

import google.auth.transport.requests
import google.oauth2.id_token
import requests
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task
from google.cloud import bigquery, storage
import pandas as pd


@task(retries=2, retry_delay_seconds=120)
def cloud_function_request(
  url: str,
  credential: Any = None,
  request_type: str = "GET",
  body_params: Any = None,
  query_params: dict | None = None,
  header_params: dict | None = None,
  env: str = "dev",
  api_type: str = "json",
  endpoint_for_filename: str | None = None,
  timeout: int = 90,
) -> dict:
  """
  Sends a request to an endpoint through a Cloud Function.

  This method is used when the endpoint is only accessible through a fixed IP.

  Args:
      url: Target endpoint URL.
      credential: Credential forwarded to the Cloud Function.
      request_type: Request method used by the Cloud Function against the target endpoint.
      body_params: Body parameters forwarded to the Cloud Function.
      query_params: Query parameters forwarded to the Cloud Function.
      header_params: Header parameters forwarded to the Cloud Function.
      env: Runtime environment.
      api_type: API type handled by the Cloud Function. Examples: json, xml, sisreg.
      endpoint_for_filename: Optional descriptor used by the Cloud Function when saving files.
      timeout: Timeout, in seconds, for the Cloud Function request.

  Returns:
      dict: Response payload returned by the Cloud Function.

  Raises:
      ValueError: If env is invalid.
      RuntimeError: If the Cloud Function request fails or GCS download fails.
  """

  if env in ["prod", "local-prod", "dev", "staging"]:
    cloud_function_url = "https://us-central1-rj-sms-dev.cloudfunctions.net/vitacare"
  else:
    raise ValueError("env must be 'prod', 'local-prod', 'dev' or 'staging'")

  request = google.auth.transport.requests.Request()
  token = google.oauth2.id_token.fetch_id_token(request, cloud_function_url)

  query_params_for_cf = query_params.copy() if query_params else {}

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

  try:
    response = requests.request(
      method="POST",
      url=cloud_function_url,
      headers=headers,
      data=json.dumps(payload),
      timeout=timeout,
    )

    if response.status_code != 200:
      message = (
        f"[Cloud Function] Request failed: {response.status_code} - {response.reason}"
      )
      log(message)
      raise RuntimeError(message)

    log("[Cloud Function] Request was successful")

    response_payload = response.json()

    if "gcs_url" in response_payload:
      gcs_url = response_payload["gcs_url"]
      log(f"[Cloud Function] GCS URL received. Downloading from: {gcs_url}")

      try:
        downloaded_content = _download_gcs_text(gcs_url=gcs_url)

        if api_type == "json":
          response_payload["body"] = json.loads(downloaded_content)
        else:
          response_payload["body"] = downloaded_content

      except Exception as exc:
        message = f"[Cloud Function] Failed to download data from GCS ({gcs_url}): {exc}"
        log(message)
        raise RuntimeError(message) from exc

    if response_payload["status_code"] != 200:
      log(
        "[Target Endpoint] Request failed: "
        f"{response_payload['status_code']} - {response_payload['body']}"
      )
    else:
      log("[Target Endpoint] Request was successful")

    return response_payload

  except Exception as exc:
    raise RuntimeError(
      f"[Cloud Function] Request failed with unknown error: {exc}"
    ) from exc


def _download_gcs_text(gcs_url: str) -> str:
  """
  Downloads a text object from a GCS URL.

  Args:
      gcs_url: GCS URL in the format gs://bucket/blob.

  Returns:
      str: Downloaded object content.
  """
  path_parts = gcs_url.replace("gs://", "").split("/", 1)

  if len(path_parts) < 2:
    raise ValueError(f"Invalid GCS URL format: {gcs_url}")

  bucket_name = path_parts[0]
  blob_name = path_parts[1]

  client = storage.Client()
  bucket = client.bucket(bucket_name)
  blob = bucket.blob(blob_name)

  return blob.download_as_text()


@task
def load_file_from_bigquery(
  project_name: str, dataset_name: str, table_name: str, environment: str = "dev"
) -> pd.DataFrame:
  """
  Load data from a BigQuery table into a pandas DataFrame.

  Args:
      project_name: BigQuery project name.
      dataset_name: BigQuery dataset name.
      table_name: BigQuery table name.
      environment: Runtime environment. Kept for compatibility.

  Returns:
      DataFrame loaded from the BigQuery table.
  """

  client = bigquery.Client()

  log(f"[Ignore] Using Parameter to avoid Warnings: {environment}")

  dataset_ref = bigquery.DatasetReference(project_name, dataset_name)

  table_ref = dataset_ref.table(table_name)

  table = client.get_table(table_ref)

  return client.list_rows(table).to_dataframe()
