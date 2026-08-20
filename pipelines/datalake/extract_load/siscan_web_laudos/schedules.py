# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from prefect.schedules import Interval

from pipelines.constants import constants as global_constants
from pipelines.utils.schedules import create_schedule_list

EXAM_PARAMETERS = [
  {"opcao_exame": "mamografia", "bq_table": "laudos_mamografia"},
  {"opcao_exame": "histo_mama", "bq_table": "laudos_histo_mama"},
  # "cito_mama" tem poucos resultados; "cito_colo" e "histo_colo"
  # ficam fora porque, por enquanto, o pipeline trabalha apenas com mama.
]

daily_manager_parameters = [
  {
    "environment": "prod",
    "relative_date": "D-5",
    "range": 5,
    "bq_dataset": "brutos_siscan_web",
    **exam_parameters,
  }
  for exam_parameters in EXAM_PARAMETERS
]

monthly_manager_parameters = [
  {
    "environment": "prod",
    "relative_date": "M-1",
    "range": 7,
    "bq_dataset": "brutos_siscan_web",
    **exam_parameters,
  }
  for exam_parameters in EXAM_PARAMETERS
]

schedules = [
  *create_schedule_list(
    parameters_list=daily_manager_parameters,
    interval="daily",
    config={"hour": 20, "minute": 0},
    interval_minutes=0,
  ),
  *[
    Interval(
      timedelta(weeks=4),
      anchor_date=datetime(2025, 8, 6, 20, 0, tzinfo=global_constants.TIMEZONE.value),
      timezone=global_constants.TIMEZONE_NAME.value,
      parameters=parameters,
    )
    for parameters in monthly_manager_parameters
  ],
]
