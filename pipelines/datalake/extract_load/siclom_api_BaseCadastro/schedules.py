# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "environment": "dev",
    "endpoint": "/mostraPaciente/",
    "annual": False,
    "year": None,
    "month": None,
    "extraction_range": None,
    "dataset_id": "brutos_siclom_api",
    "table_id": "cadastro",
  }
]

schedules = create_schedule_list(
  parameters_list=flow_parameters,
  interval="monthly",
  config={"day": 1, "hour": 3, "minute": 0},
)
