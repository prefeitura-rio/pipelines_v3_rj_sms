# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "target_date": "d-0",
  }
]

schedules = create_schedule_list(
  parameters_list=flow_parameters,
  interval="weekly",
  config={"weekday": "saturday", "hour": 3, "minute": 0},
)
