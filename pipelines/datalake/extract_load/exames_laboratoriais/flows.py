# -*- coding: utf-8 -*-
from prefect.futures import wait

from pipelines.constants import CIT
from pipelines.datalake.extract_load.exames_laboratoriais.tasks import (
  build_extraction_params,
  extract_exames_laboratoriais,
  generate_time_windows,
  get_all_aps,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import from_relative_date
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules


@flow(
  name="Extração: Exames Laboratoriais",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def exames_laboratoriais(
  dataset: str = "brutos_exames_laboratoriais",
  environment: str = "dev",
  intervalo: str = "D-1",
  hours_per_window: int | float = 2,
  end_date: str = None,
  concurrency_limit: int = 5,
):
  if concurrency_limit < 1:
    raise ValueError("concurrency_limit deve ser maior ou igual a 1")

  start_date = from_relative_date(relative_date=intervalo)
  windows = generate_time_windows(
    start_date=start_date, hours_per_window=hours_per_window, end_date=end_date
  )
  aps_list = get_all_aps()
  extraction_parameters = build_extraction_params(
    windows=windows, aps=aps_list, env=environment, dataset=dataset
  )

  failed_parameters = []

  for start_index in range(0, len(extraction_parameters), concurrency_limit):
    parameter_batch = extraction_parameters[
      start_index : start_index + concurrency_limit
    ]
    extraction_futures = [
      extract_exames_laboratoriais.submit(**parameters)
      for parameters in parameter_batch
    ]

    wait(extraction_futures)

    upload_futures = []
    for parameters, extraction_future in zip(parameter_batch, extraction_futures):
      try:
        result = extraction_future.result()
      except Exception as exc:
        failed_parameters.append({**parameters, "error": str(exc)})
        log(
          "Extração de exames laboratoriais falhou; seguindo com as próximas "
          f"janelas/APs: {parameters}. Erro: {str(exc)}",
          level="error",
        )
        continue

      solicitacoes_upload_task = upload_df_to_datalake_task.submit(
        df=result["solicitacoes_df"],
        dataset_id=result["dataset"],
        table_id="solicitacoes",
        dump_mode="append",
        source_format="parquet",
        date_partition_column="datalake_loaded_at",
      )

      exames_upload_task = upload_df_to_datalake_task.submit(
        df=result["exames_df"],
        dataset_id=result["dataset"],
        table_id="exames",
        dump_mode="append",
        source_format="parquet",
        date_partition_column="datalake_loaded_at",
        wait_for=[solicitacoes_upload_task],
      )

      resultados_upload_task = upload_df_to_datalake_task.submit(
        df=result["resultados_df"],
        dataset_id=result["dataset"],
        table_id="resultados",
        dump_mode="append",
        source_format="parquet",
        date_partition_column="datalake_loaded_at",
        wait_for=[exames_upload_task],
      )
      upload_futures.append((parameters, resultados_upload_task))

    wait([future for _, future in upload_futures])

    for parameters, upload_future in upload_futures:
      try:
        upload_future.result()
      except Exception as exc:
        failed_parameters.append({**parameters, "error": str(exc)})
        log(
          "Carga de exames laboratoriais falhou; seguindo com as próximas "
          f"janelas/APs: {parameters}. Erro: {str(exc)}",
          level="error",
        )

  if failed_parameters:
    log(
      f"{len(failed_parameters)} execuções de exames laboratoriais falharam. "
      "O fluxo preserva o comportamento do v1 e não interrompe as demais execuções.",
      level="warning",
    )


_flows = [flow_config(flow=exames_laboratoriais, schedules=schedules)]
