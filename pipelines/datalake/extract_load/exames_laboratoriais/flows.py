# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.datalake.extract_load.exames_laboratoriais.tasks import (
  authenticate_fetch,
  build_operator_params,
  generate_time_windows,
  get_all_aps,
  get_credential_param,
  get_source_from_ap,
  parse_identificador,
  transform,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules


def _run_exames_laboratoriais_operator(
  ap: str = "10",
  environment: str = "dev",
  dt_inicio: str = "2025-10-21T10:00:00-0300",
  dt_fim: str = "2025-10-21T11:30:00-0300",
  rename_flow: bool = True,
  dataset: str = "brutos_exames_laboratoriais",
):
  source = get_source_from_ap(ap=ap)
  credential = get_credential_param(source=source)

  infisical_path = credential["INFISICAL_PATH"]

  username_secret = get_secret(
    secret_name=credential["INFISICAL_USERNAME"],
    path=infisical_path,
    environment=environment,
  )
  password_secret = get_secret(
    secret_name=credential["INFISICAL_PASSWORD"],
    path=infisical_path,
    environment=environment,
  )
  apccodigo_secret = get_secret(
    secret_name=credential["INFISICAL_APCCODIGO"],
    path=infisical_path,
    environment=environment,
  )
  identificador_lis_secret = get_secret(
    secret_name=credential["INFISICAL_AP_LIS"],
    path=infisical_path,
    environment=environment,
  )
  base_url_secret = get_secret(
    secret_name=credential["INFISICAL_BASE_URL"],
    path=infisical_path,
    environment=environment,
  )

  if rename_flow:
    rename_flow_run(new_name=f"(AP{ap}) INÍCIO: {dt_inicio} FIM: {dt_fim}")

  identificador_lis = parse_identificador(identificador=identificador_lis_secret, ap=ap)

  results = authenticate_fetch(
    username=username_secret,
    password=password_secret,
    apccodigo=apccodigo_secret,
    identificador_lis=identificador_lis,
    dt_start=dt_inicio,
    dt_end=dt_fim,
    environment=environment,
    source=source,
    base_url=base_url_secret,
  )

  solicitacoes_df, exames_df, resultados_df = transform(
    json_result=results, source=source
  )

  solicitacoes_upload_task = upload_df_to_datalake_task(
    df=solicitacoes_df,
    dataset_id=dataset,
    table_id="solicitacoes",
    dump_mode="append",
    source_format="parquet",
    date_partition_column="datalake_loaded_at",
  )

  exames_upload_task = upload_df_to_datalake_task(
    df=exames_df,
    dataset_id=dataset,
    table_id="exames",
    dump_mode="append",
    source_format="parquet",
    date_partition_column="datalake_loaded_at",
    wait_for=[solicitacoes_upload_task],
  )

  upload_df_to_datalake_task(
    df=resultados_df,
    dataset_id=dataset,
    table_id="resultados",
    dump_mode="append",
    source_format="parquet",
    date_partition_column="datalake_loaded_at",
    wait_for=[exames_upload_task],
  )


@flow(
  name="DataLake - Extração e Carga de Dados - Exames Laboratoriais (Operator)",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def exames_laboratoriais_operator(
  ap: str = "10",
  environment: str = "dev",
  dt_inicio: str = "2025-10-21T10:00:00-0300",
  dt_fim: str = "2025-10-21T11:30:00-0300",
  rename_flow: bool = True,
  dataset: str = "brutos_exames_laboratoriais",
):
  _run_exames_laboratoriais_operator(
    ap=ap,
    environment=environment,
    dt_inicio=dt_inicio,
    dt_fim=dt_fim,
    rename_flow=rename_flow,
    dataset=dataset,
  )


@flow(
  name="DataLake - Extração e Carga de Dados - Exames Laboratoriais (Manager)",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def exames_laboratoriais_manager(
  dataset: str = "brutos_exames_laboratoriais",
  environment: str = "dev",
  intervalo: str = "D-1",
  hours_per_window: int = 2,
  end_date: str = None,
):
  start_date = from_relative_date(relative_date=intervalo)
  windows = generate_time_windows(
    start_date=start_date, hours_per_window=hours_per_window, end_date=end_date
  )
  aps_list = get_all_aps()
  operator_parameters = build_operator_params(
    windows=windows, aps=aps_list, env=environment, dataset=dataset
  )

  failed_parameters = []
  for parameters in operator_parameters:
    try:
      _run_exames_laboratoriais_operator(**parameters, rename_flow=False)
    except Exception as exc:
      failed_parameters.append({**parameters, "error": str(exc)})
      log(
        "Operator de exames laboratoriais falhou; seguindo com as próximas janelas/APs: "
        f"{parameters}. Erro: {str(exc)}",
        level="error",
      )

  if failed_parameters:
    log(
      f"{len(failed_parameters)} execuções do operator falharam. "
      "O manager preserva o comportamento do v1 e não interrompe as demais execuções.",
      level="warning",
    )


_flows = [
  flow_config(flow=exames_laboratoriais_operator),
  flow_config(flow=exames_laboratoriais_manager, schedules=schedules),
]
