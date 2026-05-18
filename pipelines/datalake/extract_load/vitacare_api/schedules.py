# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule


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

schedules = [
  create_schedule(
    parameters=parameters,
    interval="daily",
    config={"hour": 2 + (index // 2), "minute": 30 * (index % 2)},
  )
  for index, parameters in enumerate(routine_flow_parameters)
]
