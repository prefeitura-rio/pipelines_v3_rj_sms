# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules
from .tasks import foo


@flow(
  name="DataLake - Extração e Carga de Dados - 1746",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.CIT_ID.value],
)
def extract_1746_web(
  target_date: str,
  rename_flow: bool = True,
  environment: str = "dev",
):
  #####################################
  # Configura ambiente
  ####################################
  if rename_flow:
    rename_flow_run(new_name=f"Dump: {target_date}")

    print("Flow Teste do Pedro")
  
  foo(text="bar")

_flows = [flow_config(flow=extract_1746_web, schedules=schedules)]
