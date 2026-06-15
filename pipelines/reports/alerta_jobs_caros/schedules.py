# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={"environment": "prod"}, interval="daily", config={"hour": 10, "minute": 0}
  )
]
