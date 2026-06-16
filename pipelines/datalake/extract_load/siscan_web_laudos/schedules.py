# -*- coding: utf-8 -*-

from pipelines.utils.schedules import create_schedule, create_schedule_list

EXAM_PARAMETERS = [
  {
    "opcao_exame": "mamografia",
    "bq_table": "laudos_mamografia",
  },
  {
    "opcao_exame": "histo_mama",
    "bq_table": "laudos_histo_mama",
  },
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
    create_schedule(
      parameters=parameters,
      interval="monthly",
      config={"day": 6, "hour": 20, "minute": 0},
    )
    for parameters in monthly_manager_parameters
  ],
]
