# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from prefect.schedules import Interval

from pipelines.constants import constants

schedules = [
  Interval(
    timedelta(minutes=30),
    anchor_date=datetime(2026, 1, 1, 0, 0, tzinfo=constants.TIMEZONE.value),
    timezone=constants.TIMEZONE_NAME.value,
    parameters={"environment": "prod"},
  )
]
