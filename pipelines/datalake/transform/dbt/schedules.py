# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from prefect.schedules import Interval

from pipelines.constants import constants
from pipelines.utils.schedules import create_schedule


daily_schedule = [
  create_schedule(
    parameters={
      "command": "build",
      "environment": "prod",
      "rename_flow": True,
      "select": "tag:daily",
    },
    interval="daily",
    config={"hour": 6, "minute": 30},
  ),
  create_schedule(
    parameters={
      "command": "source freshness",
      "environment": "prod",
      "rename_flow": True,
    },
    interval="daily",
    config={"hour": 6, "minute": 30},
  ),
]

weekly_schedule = [
  create_schedule(
    parameters={
      "command": "build",
      "environment": "prod",
      "rename_flow": True,
      "select": "tag:weekly",
    },
    interval="weekly",
    config={"weekday": "domingo", "hour": 6, "minute": 20},
  )
]

monthly_schedule = [
  create_schedule(
    parameters={
      "command": "build",
      "environment": "prod",
      "rename_flow": True,
      "select": "tag:monthly",
    },
    interval="monthly",
    config={"day": 15, "hour": 6, "minute": 20},
  )
]

every_4_hours_schedule = [
  Interval(
    timedelta(hours=4),
    anchor_date=datetime(2026, 1, 1, 9, 0, tzinfo=constants.TIMEZONE.value),
    timezone=constants.TIMEZONE_NAME.value,
    parameters={
      "command": "build",
      "environment": "prod",
      "rename_flow": True,
      "select": "tag:cdi-4hours",
      "send_discord_report": False,
    },
  )
]


schedules = [
  *daily_schedule,
  *weekly_schedule,
  *monthly_schedule,
  *every_4_hours_schedule,
]
