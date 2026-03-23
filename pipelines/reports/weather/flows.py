# -*- coding: utf-8 -*-
from pipelines.utils.flow import flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.constants import constants as consts

from .tasks import fetch_weather, get_bairros, print_report
from .schedules import schedules


@flow(
	name="Report: Previsão do Tempo",
	state_handlers=[handle_flow_state_change],
	owners=[consts.AVELLAR_ID.value],
)
def weather_report(lat: float, lon: float, environment: str = "dev"):
	zero = get_bairros()
	data = fetch_weather(lat=lat + zero, lon=lon, environment=environment)
	print_report(data=data)


_flows = [weather_report]
_schedules = schedules
