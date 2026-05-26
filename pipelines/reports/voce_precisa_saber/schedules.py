# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={
      "environment": "prod"
      # "date": (não passamos porque queremos 'hoje'),
    },
    interval="daily",
    config={"hour": 6, "minute": 0},
  )
]
