# -*- coding: utf-8 -*-
from prefect.concurrency.sync import rate_limit
from prefect.futures import PrefectFuture, wait

from pipelines.constants import constants as global_consts
from pipelines.datalake.extract_load.siclom_api.constants import (
  constants as siclom_constants,
)
from pipelines.datalake.extract_load.siclom_api.tasks import (
  format_month,
  generate_formatted_months,
  get_siclom_period_data,
  get_siclom_prep_data,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules


@flow(
  name="DataLake - Extração e Carga de Dados - SICLOM API",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.HERIAN_ID.value],
)
def siclom_period_extraction(
  environment: str = "dev",  # required=True in v1
  endpoint: str = None,  # required=True in v1
  table_id: str = "",  # required=True in v1
  dataset_id: str = "brutos_siclom_api",  # required=True in v1
  annual: bool = False,  # required=True in v1
  month: str = None,
  year: str = None,
  extraction_range: int = 5,
):
  """
  Os parametros annual, month, year e extraction_range foram pensados para permitir flexibilidade nas extrações.

  Por exemplo, se quisermos extrair os dados de um mês específico, basta preencher os campos month e year,
  deixando annual como False. Se quisermos extrair os dados de um ano específico, basta preencher o campo year
  e annual como True, o campo month será ignorado.

  O campo extraction_range serve para definir o intervalo de anos a ser extraído a partir do ano de referência,
  ou seja, se year for 2024 e extraction_range for 5, serão extraídos os dados de 2024, 2023, 2022, 2021 e 2020.
  """

  BASE_URL = get_secret(
    secret_name=siclom_constants.URL.value,
    path=siclom_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  API_KEY = get_secret(
    secret_name=siclom_constants.APY_KEY.value,
    path=siclom_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  rename_flow_run(new_name=f"{table_id} - annual={annual} - {environment}")

  if not annual:
    # 1 - Formata a string do mês
    formated_month = format_month(month=month, year=year)

    # 2 - Faz a requisição na API do SICLOM
    if endpoint != "/resultadoprepperiodo/":
      month_data = get_siclom_period_data(
        base_url=BASE_URL, endpoint=endpoint, api_key=API_KEY, period=formated_month
      )
    else:
      month_data = get_siclom_prep_data(
        base_url=BASE_URL, endpoint=endpoint, api_key=API_KEY, period=formated_month
      )

    # 3 - Carrega os dados no datalake
    upload_df_to_datalake_task(
      df=month_data,
      dataset_id=dataset_id,
      table_id=table_id,
      dump_mode="append",
      date_partition_column="extracted_at",
      csv_delimiter=",",
    )

  else:
    # 1 - Gera as strings numéricas de cada mês formatadas
    formated_periods = generate_formatted_months(
      reference_year=year, interval=extraction_range
    )

    # 2 - Faz as requisições na API do SICLOM
    months_data_futures: list[PrefectFuture] = []
    for period in formated_periods:
      rate_limit("um-por-segundo")
      if endpoint != "/resultadoprepperiodo/":
        months_data_futures.append(
          get_siclom_period_data.submit(
            base_url=BASE_URL, period=period, endpoint=endpoint, api_key=API_KEY
          )
        )
      else:
        months_data_futures.append(
          get_siclom_prep_data.submit(
            base_url=BASE_URL, period=period, endpoint=endpoint, api_key=API_KEY
          )
        )

    # Espera todas as extrações terminarem
    wait(months_data_futures)
    months_data = [future.result() for future in months_data_futures]

    # 3 - Carrega os dados no datalake
    upload_futures = []
    for df in months_data:
      rate_limit("um-por-segundo")
      upload_futures.append(
        upload_df_to_datalake_task.submit(
          df=df,
          dataset_id=dataset_id,
          table_id=table_id,
          dump_mode="append",
          date_partition_column="extracted_at",
          csv_delimiter=",",
        )
      )

    # Espera todos os uploads terminarem
    wait(upload_futures)


# num_workers=2, memory_limit="4Gi", memory_request="2Gi"
_flows = [flow_config(flow=siclom_period_extraction, schedules=schedules)]
