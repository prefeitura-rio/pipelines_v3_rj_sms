# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedule = [
  create_schedule(
    parameters={"environment": "prod"}, interval="daily", config={"hour": 8, "minute": 55}
  ),
  create_schedule(
    parameters={"environment": "prod"},
    interval="daily",
    config={"hour": 17, "minute": 50},
  ),
]
