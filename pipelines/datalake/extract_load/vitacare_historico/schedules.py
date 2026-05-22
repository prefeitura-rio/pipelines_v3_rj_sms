# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [{"environment": "dev"}]

schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters,
    interval="monthly",
    config={"day": 8, "hour": 21, "minute": 0},
  )
]
