from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "bucket_name": "subhue_backups",
    "chunk_size": 1000,
    "dataset": "brutos_prontuario_prontuaRio_staging",
    "environment": "prod",
    "folder": "",
  }
]

schedules = create_schedule_list(
  parameters_list=flow_parameters,
  interval="weekly",
  config={"weekday": "sunday", "hour": 22, "minute": 0},
)
