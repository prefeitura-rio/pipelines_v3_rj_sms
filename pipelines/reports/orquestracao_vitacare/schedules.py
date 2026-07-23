# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule

schedules = [
    create_schedule(
        parameters={"environment": "prod"},
        interval="monthly",
        config={"day": 7, "hour": 16, "minute": 0},
    )
]
