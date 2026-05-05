# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list


flow_parameters = [
  {
    "environment": "prod",
    "rename_flow": True,
    "db_url_infisical_key": "PRONTUARIOS_DB_URL",
    "db_url_infisical_path": "/",
    "source_schema_name": "public",
    "source_table_name": "userhistory",
    "source_datetime_column": "timestamp",
    "target_dataset_id": "brutos_aplicacao_hci",
    "target_table_id": "userhistory",
    "relative_datetime": "D-1",
    "historical_mode": True,
  },
  {
    "environment": "prod",
    "rename_flow": True,
    "db_url_infisical_key": "PRONTUARIOS_DB_URL",
    "db_url_infisical_path": "/",
    "source_schema_name": "public",
    "source_table_name": "user",
    "target_dataset_id": "brutos_aplicacao_hci",
    "target_table_id": "user",
    "historical_mode": True,
  },
]


schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters,
    interval="daily",
    config={"hour": 5},
  ),
]
