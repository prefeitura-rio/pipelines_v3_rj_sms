# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from prefect.schedules import Interval

from pipelines.constants import constants as global_constants

routine_flow_parameters = [
  {
    "dataset_id": "brutos_prontuario_vitacare_api_centralizadora",
    "endpoint": "posicao",
    "environment": "prod",
    "rename_flow": True,
    "table_id_prefix": "estoque_posicao",
    "target_date": "D-0",
  },
  {
    "dataset_id": "brutos_prontuario_vitacare_api_centralizadora",
    "endpoint": "movimento",
    "environment": "prod",
    "rename_flow": True,
    "table_id_prefix": "estoque_movimento",
    "target_date": "D-1",
  },
  {
    "dataset_id": "brutos_prontuario_vitacare_api_centralizadora",
    "endpoint": "vacina",
    "environment": "prod",
    "rename_flow": True,
    "table_id_prefix": "vacinacao",
    "target_date": "D-3",
  },
  {
    "dataset_id": "brutos_prontuario_vitacare_api_centralizadora",
    "endpoint": "movimento",
    "environment": "prod",
    "rename_flow": True,
    "table_id_prefix": "estoque_movimento",
    "target_date": "D-7",
  },
  {
    "dataset_id": "brutos_prontuario_vitacare_api_centralizadora",
    "endpoint": "vacina",
    "environment": "prod",
    "rename_flow": True,
    "table_id_prefix": "vacinacao",
    "target_date": "D-7",
  },
]


def create_daily_schedule(parameters: dict, offset_minutes: int) -> Interval:
  anchor_date = datetime(
    2026, 1, 1, 2, 0, tzinfo=global_constants.TIMEZONE.value
  ) + timedelta(minutes=offset_minutes)
  return Interval(
    timedelta(days=1),
    anchor_date=anchor_date,
    timezone=global_constants.TIMEZONE_NAME.value,
    parameters=parameters,
  )


schedules = [
  create_daily_schedule(parameters=parameters, offset_minutes=index * 30)
  for index, parameters in enumerate(routine_flow_parameters)
]
