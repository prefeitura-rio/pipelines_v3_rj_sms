# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "recurso": "RECURSO_FICHA_TUBERCULOSE",
    "data_inicio": "D-1",
    "environment": "prod",
  },
  {
    "recurso": "RECURSO_FICHA_VIOLENCIA",
    "data_inicio": "D-1",
    "environment": "prod",
  },
  {
    "recurso": "RECURSO_EXAMES_LABORATORIAIS",
    "data_inicio": "D-1",
    "environment": "prod",
  },
]

schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters,
    interval="daily",
    config={"hour": 3, "minute": 30},
    interval_minutes=30,
  )
]
