# -*- coding: utf-8 -*-
import pandas as pd
from google.cloud import bigquery

from pipelines.utils.api import convert_usd_to_brl
from pipelines.utils.monitor import send_discord_message
from pipelines.utils.prefect import authenticated_task as task


@task
def get_recent_bigquery_jobs(
  environment: str, cost_threshold: float = 0.5, time_threshold: int = 24
):
  query = f"""
    SELECT
      user.email,
      creation_time,
      user.name,
      job_id,
      destination.project_id,
      destination.dataset_id,
      destination.table_id,
      billing_estimated_charge_in_usd as custo_dolar_estimado
    FROM `rj-sms.dashboard_infraestrutura.queries`
    WHERE
      creation_time >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_threshold} HOUR) and
      billing_estimated_charge_in_usd > {cost_threshold} and
      user.email not like 'prefect%'
    ORDER BY custo_dolar_estimado desc
    """
  client = bigquery.Client()
  query_job = client.query(query)
  results = query_job.result().to_dataframe()

  results["custo_real_estimado"] = convert_usd_to_brl(usd=1) * results["custo_dolar_estimado"]

  return results


@task
def send_discord_alert(environment: str, results: pd.DataFrame):
  if results.empty:
    send_discord_message(
      title="Alerta de Jobs Caros no BigQuery nas últimas 24h",
      message="Nenhum job caro encontrado nas últimas 24h",
      slug="custo_jobs",
    )
    return

  lines = []
  for _, row in results.iterrows():
    time = row.creation_time.strftime("%H:%M:%S")
    custo = f"R$ {row.custo_real_estimado:.2f}"
    link = f"https://console.cloud.google.com/bigquery?project={row.project_id}&j=bq:US:{row.job_id}&page=queryresults"  # noqa: E501
    user = row.email.split("@")[0] + "@..."
    if not row.dataset_id:
      if len(row.table_id) > 20:
        destination = f"`{row.table_id[:20]}...`"
      else:
        destination = f"`{row.table_id}`"
    else:
      destination = f"`{row.dataset_id}`.`{row.table_id}`"

    if row.custo_real_estimado > 5:
      emoji = "⚠️"
    else:
      emoji = ""

    line = f"""- [{time}] {emoji} **{custo}** por `{user}` para {destination}. [Ver detalhes do job]({link})"""  # noqa: E501
    lines.append(line)

  send_discord_message(
    title="Alerta de Jobs Caros no BigQuery nas últimas 24h",
    message="\n".join(lines),
    slug="custo_jobs",
  )
