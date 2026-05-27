# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={"environment": "dev"},
    interval="monthly",
    config={"day": 8, "hour": 21, "minute": 0},
  )
]
