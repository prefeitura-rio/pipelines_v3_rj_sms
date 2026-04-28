# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={
      "environment": "prod"
      # "date": (não passamos porque queremos 'ontem'),
      # "override_recipients": (não passamos por motivos óbvios)
    },
    interval="daily",
    config={"hour": 7, "minute": 30},
  )
]
