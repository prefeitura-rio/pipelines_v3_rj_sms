# -*- coding: utf-8 -*-
from prefect.schedules import Cron

from pipelines.constants import constants

schedules = [
  Cron(
    "0 6 * * 1-5",  # Às 6:00, todo dia, todo mês, SEG-SEX
    timezone=constants.TIMEZONE_NAME.value,
    parameters={
      "environment": "prod"
      # "date": (não passamos porque queremos 'hoje'),
    },
  )
]
