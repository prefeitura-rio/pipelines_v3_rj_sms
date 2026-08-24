# -*- coding: utf-8 -*-
from pipelines.datalake.extract_load.sarah_padi.constants import (
  constants as padi_constants,
)
from pipelines.utils.schedules import create_schedule_list

TABLES = padi_constants.TABLES.value

daily_parameters = [
  {
    "table": table,
    "date": None,  # Neste caso pega D-1
    "dataset_id": "brutos_prontuario_sarah_padi",
    "environment": "prod",
  }
  for table in TABLES.keys()
]

schedules = create_schedule_list(
  parameters_list=daily_parameters,
  interval="daily",
  config={"hour": 0, "minute": 5},
  interval_minutes=1,
)
