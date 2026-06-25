# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={"keep_old_files": False, "environment": "prod"},
    interval="daily",
    config={"hour": 3, "minute": 30},
  )
]
