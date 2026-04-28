# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "environment": "prod",
    "endpoint": "/mostracargacd4periodo/",
    "annual": False,
    "year": None,
    "month": None,
    "extraction_range": None,
    "dataset_id": "brutos_siclom_api",
    "table_id": "carga_cd4",
  },
  {
    "environment": "prod",
    "endpoint": "/mostracargaviralperiodo/",
    "annual": False,
    "year": None,
    "month": None,
    "extraction_range": None,
    "dataset_id": "brutos_siclom_api",
    "table_id": "carga_viral",
  },
  {
    "environment": "prod",
    "endpoint": "/tratamentoperiodo/",
    "annual": False,
    "year": None,
    "month": None,
    "extraction_range": None,
    "dataset_id": "brutos_siclom_api",
    "table_id": "tratamento",
  },
  {
    "environment": "prod",
    "endpoint": "/resultadopepperiodo/",
    "annual": False,
    "year": None,
    "month": None,
    "extraction_range": None,
    "dataset_id": "brutos_siclom_api",
    "table_id": "pep",
  },
  {
    "environment": "prod",
    "endpoint": "/resultadoprepperiodo/",
    "annual": False,
    "year": None,
    "month": None,
    "extraction_range": None,
    "dataset_id": "brutos_siclom_api",
    "table_id": "prep",
  },
]

schedules = create_schedule_list(
  parameters_list=flow_parameters,
  interval="weekly",
  config={"weekday": "saturday", "hour": 3, "minute": 0},
)
