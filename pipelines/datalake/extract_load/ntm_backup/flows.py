# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.datetime import current_weekday
from pipelines.utils.infisical import get_secret_task
from pipelines.utils.prefect import flow, flow_config

from .constants import constants as flow_consts
from .schedules import schedules
from .tasks import (
  cleanup_old_gcs_files,
  copy_daily_gcs_as_weekly,
  dump_task,
  upload_daily_to_gcs,
)
from .utils import generate_filename


@flow(
  name="Extração: NTM Backup MySQL",
  owners=[CIT.AVELLAR_ID.value],
  tags=["NTM"]
)  # fmt: skip
def ntm_backup_mysql(keep_old_files: bool = False, environment: str = "dev"):
  """
  Args:
    keep_old_files(bool?):
      Por padrão, o flow irá apagar arquivos de backup antigos do bucket.
      Se `keep_old_files=True`, ele não apaga os arquivos antigos.
    environment(str?):
      Ambiente de execução ("dev", "prod").
  """
  DB_HOST = get_secret_task(secret_name="DB_HOST", environment=environment, path="/ntm")
  DB_PORT = get_secret_task(secret_name="DB_PORT", environment=environment, path="/ntm")
  DB_NAME = get_secret_task(secret_name="DB_NAME", environment=environment, path="/ntm")

  DB_USER = get_secret_task(secret_name="DB_USER", environment=environment, path="/ntm")
  DB_PASSWORD = get_secret_task(
    secret_name="DB_PASSWORD", environment=environment, path="/ntm"
  )

  BUCKET_NAME = get_secret_task(
    secret_name="GCS_BUCKET_NAME", environment=environment, path="/ntm"
  )

  CREDENTIALS = get_secret_task(
    secret_name="GOOGLE_APPLICATION_CREDENTIALS_JSON",
    environment=environment,
    path="/ntm",
  )
  with open(flow_consts.NTM_CREDENTIALS_PATH.value, "w") as f:
    f.write(CREDENTIALS)

  daily_filename = generate_filename("daily", DB_NAME)

  path = dump_task(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    filename=daily_filename,
  )

  # Upload diário
  daily_uri = upload_daily_to_gcs(BUCKET_NAME, path)

  # Se for segunda-feira, sobe também como semanal
  if current_weekday(format="pt") == "segunda":
    weekly_filename = generate_filename("weekly", DB_NAME)
    copy_daily_gcs_as_weekly(from_uri=daily_uri, filename=weekly_filename)

  cleanup_old_gcs_files(bucket_name=BUCKET_NAME, dry_run=keep_old_files)


_flows = [
  flow_config(
    flow=ntm_backup_mysql,
    schedules=schedules,
    memory="small",
    dockerfile="./pipelines/datalake/extract_load/ntm_backup/Dockerfile",
  )
]
