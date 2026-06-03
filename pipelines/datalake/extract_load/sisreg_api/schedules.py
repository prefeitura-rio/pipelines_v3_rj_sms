# -*- coding: utf-8 -*-

from pipelines.utils.schedules import create_schedule

schedules = [
  create_schedule(
    parameters={
      "es_index": "marcacao-ambulatorial-rj",
      "page_size": 5_000,
      "dias_por_faixa": 5,
      "dataset_id": "brutos_sisreg_api_v2",
      "environment": "prod",
    },
    interval="daily",
    config={"hour": 23, "minute": 45},
  ),
  create_schedule(
    parameters={
      "es_index": "solicitacao-ambulatorial-rj",
      "page_size": 5_000,
      "dias_por_faixa": 5,
      "dataset_id": "brutos_sisreg_api_v2",
      "environment": "prod",
    },
    interval="daily",
    config={"hour": 23, "minute": 30},
  ),
  create_schedule(
    parameters={
      "es_index": "solicitacao-hospitalar-rj",
      "page_size": 5_000,
      "dias_por_faixa": 5,
      "dataset_id": "brutos_sisreg_api_v2",
      "environment": "prod",
    },
    interval="daily",
    config={"hour": 23, "minute": 15},
  ),
]
