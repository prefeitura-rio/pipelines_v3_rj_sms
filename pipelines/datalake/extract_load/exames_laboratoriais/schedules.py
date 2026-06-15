# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "environment": "prod",
    "intervalo": "D-1",
    "dataset": "brutos_exames_laboratoriais",
    "hours_per_window": 1.5,
  }
]

schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters, interval="daily", config={"hour": 4, "minute": 0}
  )
]
