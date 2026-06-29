# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config, rename_flow_run

from .schedules import schedules
from .tasks import foo


@flow(name="Extração: 1746 Web", owners=[CIT.CIT_ID.value], tags=["CIT"])
def extract_1746_web(
  target_date: str, rename_flow: bool = True, environment: str = "dev"
):
  #####################################
  # Configura ambiente
  ####################################
  if rename_flow:
    rename_flow_run(new_name=f"Dump: {target_date}")

    print("Flow Teste do Pedro")

  foo(text="bar")


_flows = [flow_config(flow=extract_1746_web, schedules=schedules)]
