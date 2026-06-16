# -*- coding: utf-8 -*-
from prefect.futures import wait

from pipelines.constants import CIT, SUBGERAL
from pipelines.datalake.extract_load.siscan_web_laudos.constants import (
  constants as siscan_constants,
)
from pipelines.datalake.extract_load.siscan_web_laudos.tasks import (
  build_operator_parameters,
  extract_siscan_laudos,
  generate_extraction_windows,
  parse_date,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import (
  create_flow_run,
  flow,
  flow_config,
  rename_flow_run,
  wait_for_flow_run_task,
)

from .schedules import schedules


@flow(
  name="Extração: SISCAN Web Laudos (Operator)",
  owners=[SUBGERAL.MILOSKI_ID.value, CIT.HERIAN_ID.value],
  tags=["SUBGERAL", "CIT"],
)
def sms_siscan_web_operator(
  environment: str = "dev",
  opcao_exame: str = siscan_constants.DEFAULT_EXAM_OPTION.value,
  data_inicial: str = "01/01/2025",
  data_final: str = "31/01/2025",
  bq_dataset: str = siscan_constants.DEFAULT_DATASET_ID.value,
  bq_table: str = siscan_constants.DEFAULT_TABLE_ID.value,
):
  rename_flow_run(
    new_name=(
      f"{opcao_exame}: {data_inicial} a {data_final} -> "
      f"{bq_dataset}.{bq_table} ({environment})"
    )
  )

  user = get_secret(
    secret_name=siscan_constants.INFISICAL_USERNAME.value,
    path=siscan_constants.INFISICAL_PATH.value,
    environment=environment,
  )
  password = get_secret(
    secret_name=siscan_constants.INFISICAL_PASSWORD.value,
    path=siscan_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  laudos = extract_siscan_laudos(
    email=user,
    password=password,
    opcao_exame=opcao_exame,
    start_date=data_inicial,
    end_date=data_final,
  )
  upload_df_to_datalake_task(
    df=laudos,
    dataset_id=bq_dataset,
    table_id=bq_table,
    dump_mode="append",
    source_format=siscan_constants.SOURCE_FORMAT.value,
    date_partition_column=siscan_constants.PARTITION_COLUMN.value,
    dataset_is_public=False,
  )


@flow(
  name="Extração: SISCAN Web Laudos (Manager)",
  owners=[SUBGERAL.MILOSKI_ID.value, CIT.HERIAN_ID.value],
  tags=["SUBGERAL", "CIT"],
)
def sms_siscan_web_manager(
  environment: str = "dev",
  relative_date: str = "D-1",
  range: int = 1,
  rename_flow: bool = True,
  opcao_exame: str = siscan_constants.DEFAULT_EXAM_OPTION.value,
  bq_dataset: str = siscan_constants.DEFAULT_DATASET_ID.value,
  bq_table: str = siscan_constants.DEFAULT_TABLE_ID.value,
):
  if rename_flow:
    rename_flow_run(
      new_name=(
        f"{opcao_exame}: relative_date={relative_date}, range={range} -> "
        f"{bq_dataset}.{bq_table} ({environment})"
      )
    )

  start_date = from_relative_date(relative_date=relative_date)
  windows = generate_extraction_windows(
    start_date=start_date, end_date=None, interval=range
  )
  operator_params = build_operator_parameters(
    windows=windows,
    environment=environment,
    bq_dataset=bq_dataset,
    bq_table=bq_table,
    opcao_exame=opcao_exame,
  )

  flow_runs = [
    create_flow_run(
      flow=sms_siscan_web_operator,
      parameters=parameters,
      environment=environment,
      flow_run_name=(
        f"{parameters['opcao_exame']}: "
        f"{parameters['data_inicial']} a {parameters['data_final']}"
      ),
    )
    for parameters in operator_params
  ]
  wait_futures = [
    wait_for_flow_run_task.submit(flow_run_id=flow_run.id) for flow_run in flow_runs
  ]
  wait(wait_futures)


@flow(
  name="Extração: SISCAN Web Laudos Histórico",
  owners=[SUBGERAL.MILOSKI_ID.value, CIT.HERIAN_ID.value],
  tags=["SUBGERAL", "CIT"],
)
def sms_siscan_web_historical(
  environment: str = "dev",
  range: int = 1,
  rename_flow: bool = True,
  start_date: str = "01/01/2025",
  end_date: str = "31/01/2025",
  opcao_exame: str = siscan_constants.DEFAULT_EXAM_OPTION.value,
  bq_dataset: str = siscan_constants.DEFAULT_DATASET_ID.value,
  bq_table: str = siscan_constants.DEFAULT_TABLE_ID.value,
):
  if rename_flow:
    rename_flow_run(
      new_name=(
        f"{opcao_exame}: {start_date} a {end_date}, range={range} -> "
        f"{bq_dataset}.{bq_table} ({environment})"
      )
    )

  parsed_start_date = parse_date(value=start_date)
  parsed_end_date = parse_date(value=end_date)
  windows = generate_extraction_windows(
    start_date=parsed_start_date, end_date=parsed_end_date, interval=range
  )
  operator_params = build_operator_parameters(
    windows=windows,
    environment=environment,
    bq_dataset=bq_dataset,
    bq_table=bq_table,
    opcao_exame=opcao_exame,
  )

  flow_runs = [
    create_flow_run(
      flow=sms_siscan_web_operator,
      parameters=parameters,
      environment=environment,
      flow_run_name=(
        f"{parameters['opcao_exame']}: "
        f"{parameters['data_inicial']} a {parameters['data_final']}"
      ),
    )
    for parameters in operator_params
  ]
  wait_futures = [
    wait_for_flow_run_task.submit(flow_run_id=flow_run.id) for flow_run in flow_runs
  ]
  wait(wait_futures)


# v1: operator num_workers=1, memory_request="1Gi", memory_limit="1Gi"
# v1: manager/historical num_workers=6, memory_request="2Gi", memory_limit="2Gi"
_flows = [
  flow_config(flow=sms_siscan_web_manager, schedules=schedules),
  flow_config(flow=sms_siscan_web_operator),
  flow_config(flow=sms_siscan_web_historical),
]
