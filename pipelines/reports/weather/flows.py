# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules
from .tasks import fetch_weather, get_bairros, print_report, write_bairros_to_gcs


@flow(
  name="Report: Previsão do Tempo",
  description="Prevê o tempo dado latitude/longitude :) Printa bairros do Rio também",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.AVELLAR_ID.value],
)
def weather_report(lat: float, lon: float, environment: str = "dev"):
  bairros = get_bairros()
  write_bairros_to_gcs(data=bairros)

  data = fetch_weather(lat=lat, lon=lon, environment=environment)
  print_report(data=data)


_flows = [flow_config(flow=weather_report, schedules=schedules, mount_gcs=True)]
