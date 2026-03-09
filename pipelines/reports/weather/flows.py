# -*- coding: utf-8 -*-
from pipelines.utils.flow import flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.constants import constants

from .tasks import (
	fetch_weather,
	print_report
)


@flow(
	name="Report: Previsão do Tempo",
	state_handlers=[handle_flow_state_change],
	owners=[
		constants.AVELLAR_ID.value,
	],
)
def weather_report(lat: float, lon: float, environment: str="dev"):
	data = fetch_weather(lat=lat, lon=lon, environment=environment)
	print_report(data=data)
