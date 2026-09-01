# -*- coding: utf-8 -*-

from pipelines.utils.schedules import create_schedule

schedules = [
  # mode="update"
  # Pegamos solicitações atualizadas nos últimos 7 dias
  create_schedule(
    parameters={
      "es_index": "marcacao-ambulatorial-rj",
      "page_size": 10_000,
      "dias_por_faixa": 7,
      "dataset_id": "brutos_sisreg_api_v2",
      "mode": "update",
      "environment": "prod",
    },
    interval="daily",
    config={"hour": 23, "minute": 45},
  ),
  create_schedule(
    parameters={
      "es_index": "solicitacao-ambulatorial-rj",
      "page_size": 10_000,
      "dias_por_faixa": 7,
      "dataset_id": "brutos_sisreg_api_v2",
      "mode": "update",
      "environment": "prod",
    },
    interval="daily",
    config={"hour": 23, "minute": 30},
  ),
  create_schedule(
    parameters={
      "es_index": "solicitacao-hospitalar-rj",
      "page_size": 10_000,
      "dias_por_faixa": 7,
      "dataset_id": "brutos_sisreg_api_v2",
      "mode": "update",
      "environment": "prod",
    },
    interval="daily",
    config={"hour": 23, "minute": 15},
  ),
]
