# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "db_url_infisical_key": "DB_URL",
    "db_url_infisical_path": "/hci",
    "target_dataset_id": "brutos_aplicacao_hci",
    "target_table_id": "userhistory",
    "source_schema_name": "public",
    "source_table_name": "userhistory",
    "extract_whole_table": True,
    "rename_flow": True,
    "environment": "prod",
  },
  {
    "db_url_infisical_key": "DB_URL",
    "db_url_infisical_path": "/hci",
    "target_dataset_id": "brutos_aplicacao_hci",
    "target_table_id": "user",
    "source_schema_name": "public",
    "source_table_name": "user",
    "extract_whole_table": True,
    "rename_flow": True,
    "environment": "prod",
  },
]


schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters, interval="daily", config={"hour": 5}
  )
]
