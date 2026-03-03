# -*- coding: utf-8 -*-
from prefect import flow
# from pipelines.utils.flow import FlowDecorator as flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.constants import constants

from .tasks import (
	fetch_weather,
	print_report
)


@flow(
	name="Report: Previsão do Tempo",
	on_completion=[handle_flow_state_change],
	on_cancellation=[handle_flow_state_change],
	on_crashed=[handle_flow_state_change],
	on_failure=[handle_flow_state_change],
	on_running=[handle_flow_state_change],
	# state_handlers=[handle_flow_state_change],
	# owners=[
	# 	constants.AVELLAR_ID.value,
	# ],
)
def weather_report(lat: float, lon: float, environment: str="dev"):
	data = fetch_weather(lat=lat, lon=lon, environment=environment)
	print_report(data=data)
