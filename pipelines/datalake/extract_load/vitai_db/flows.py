# -*- coding: utf-8 -*-
from prefect.concurrency.sync import rate_limit
from prefect.futures import wait

from pipelines.constants import CIT
from pipelines.datalake.extract_load.vitai_db.constants import (
  constants as vitai_db_constants,
)
from pipelines.datalake.extract_load.vitai_db.schedules import schedules
from pipelines.datalake.extract_load.vitai_db.tasks import (
  create_working_time_range,
  define_queries,
  run_query,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log  # DEV
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change


@flow(
  name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde)",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.HERIAN_ID.value],
)
def vitai_db_extraction(
  environment: str = "dev",
  table_name: str = "",
  schema_name: str = "basecentral",
  datetime_column: str = "datahora",
  target_name: str = "",
  partition_column: str = "datalake_loaded_at",
  batch_size: int = 10000,
  interval_start: str = None,
  interval_end: str = None,
  relative_date: str = None,
):
  """
  Fluxo de extração e carga de dados do prontuário Vitai.

  Args:
      environment: Ambiente de execução (dev, prod)
      table_name: Tabela fonte no banco Vitai
      schema_name: Schema da tabela fonte
      datetime_column: Coluna de referência temporal
      target_name: Nome da tabela destino no BigQuery
      partition_column: Coluna de partição
      batch_size: Tamanho do lote de extração
      interval_start: Início do intervalo. Padrão: 7 dias atrás.
      interval_end: Fim do intervalo. Padrão: Agora.
  """
  rename_flow_run(
    new_name=f"{relative_date}: '{schema_name}.{table_name}' -> '{target_name}'"
  )

  db_url = get_secret(
    secret_name="DB_URL", environment=environment, path="/prontuario-vitai"
  )

  log(f"DB_URL: {db_url}", level="debug")

  start, end = create_working_time_range(
    interval_start=interval_start, interval_end=interval_end, relative_date=relative_date
  )

  queries = define_queries(
    db_url=db_url,
    schema_name=schema_name,
    table_name=table_name,
    dt_column=datetime_column,
    interval_start=start,
    interval_end=end,
    batch_size=batch_size,
  )

  if not queries:
    return

  dataframes_futures = []
  for query in queries:
    rate_limit("um-por-segundo")
    dataframes_futures.append(
      run_query.submit(db_url=db_url, query=query, partition_column=partition_column)
    )

  wait(dataframes_futures)
  dataframes = [f.result() for f in dataframes_futures]

  upload_futures = []
  for df in dataframes:
    rate_limit("um-por-segundo")
    upload_futures.append(
      upload_df_to_datalake_task.submit(
        df=df,
        dataset_id=vitai_db_constants.DATASET_NAME.value,
        table_id=target_name,
        dump_mode="replace",
        source_format="parquet",
        biglake_table=True,
        dataset_is_public=False,
        date_partition_column=partition_column,
      )
    )

  wait(upload_futures)


_flows = [flow_config(flow=vitai_db_extraction, schedules=schedules, memory="medium")]
