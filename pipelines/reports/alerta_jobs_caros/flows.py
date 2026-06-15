# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config

from .constants import COST_THRESHOLD, TIME_THRESHOLD
from .schedules import schedules
from .tasks import get_recent_bigquery_jobs, send_discord_alert


@flow(name="Report: Alerta Jobs Caros", owners=[CIT.PEDRO_ID.value], tags=["CIT"])
def report_alerta_jobs_caros(environment: str = "dev"):
  jobs = get_recent_bigquery_jobs(
    environment=environment, cost_threshold=COST_THRESHOLD, time_threshold=TIME_THRESHOLD
  )

  send_discord_alert(environment=environment, results=jobs)


_flows = [flow_config(flow=report_alerta_jobs_caros, schedules=schedules)]
