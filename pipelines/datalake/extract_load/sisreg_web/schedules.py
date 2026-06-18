# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={
      "environment": "prod",
      "endpoint": "escala",
      "dataset_id": "brutos_sisreg",
      "table_id": "escala",
    },
    interval="daily",
    config={"hour": 6},
  )
]
