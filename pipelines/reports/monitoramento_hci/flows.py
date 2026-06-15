# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config

from .schedules import schedules
from .tasks import get_data, send_report


@flow(name="Report: Monitoramento do HCI", owners=[CIT.AVELLAR_ID.value], tags=["CIT"])
def report_uso_hci(
  dataset_id: str = "app_historico_clinico",
  table_id: str = "registros",
  environment: str = "dev",
):
  #####################################
  # Tasks
  #####################################
  data = get_data(dataset_name=dataset_id, table_name=table_id, environment=environment)
  send_report(data=data, environment=environment)


_flows = [flow_config(flow=report_uso_hci, schedules=schedules)]
