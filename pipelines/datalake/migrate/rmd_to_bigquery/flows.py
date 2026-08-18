# -*- coding: utf-8 -*-

from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config

from .constants import RecursosRMD

# from .schedules import schedules
from .tasks import calculate_date_interval, get_secrets, query_api_page, upload_data


@flow(
  name="Migração: RMD → BigQuery",
  description="Clona dados do RMD para o BigQuery",
  owners=[CIT.CIT_ID.value],
  tags=["CIT"],
)
def rmd_to_bigquery(
  recurso: RecursosRMD,
  data_inicio: str = "D-30",
  data_fim: str | None = None,
  dataset_id: str = "brutos_rmd",
  environment: str = "dev",
):
  secrets = get_secrets(recurso=recurso, environment=environment)
  (start, end) = calculate_date_interval(data_inicio=data_inicio, data_fim=data_fim)

  skip = 0
  total = None
  while True:
    page_data, total = query_api_page(
      secrets=secrets, data_inicio=start, data_fim=end, skip=skip
    )
    if page_data:
      upload_data(
        dataset_id=dataset_id, secrets=secrets, data=page_data, environment=environment
      )
    skip += len(page_data)
    if skip >= total or len(page_data) == 0:
      break


_flows = [flow_config(flow=rmd_to_bigquery)]
