# -*- coding: utf-8 -*-
from typing import Literal, Optional

from prefect.concurrency.sync import rate_limit
from prefect.futures import wait

from pipelines.constants import CIT, SUBGERAL
from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.infisical import get_secret_task
from pipelines.utils.prefect import clear_concurrency_limit, flow, flow_config

from .constants import constants as flow_constants
from .schedules import schedules
from .tasks import extract_from_api, gerar_faixas_de_data
from .utils import table_name_from_resource


# A tag de limite de concorrência pra task de extração do Sisreg
# não tem "slot decay" configurado; em caso de crash/cancelamento de flow,
# os slots não são desocupados. O Prefect tem, em teoria, algum sistema
# de GC que deveria reabrir os slots, mas eu nunca vi acontecendo.
# Então colocamos hooks pra 'manualmente' reabrir os slots caso
# o flow seja interrompido
def clear_sisreg_limit(*args, **kwargs):
  limit = f"tag:{flow_constants.CONCURRENCY_LIMIT_TAG.value}"
  clear_concurrency_limit(limit)


@flow(
  name="Extração: Sisreg API",
  owners=[CIT.AVELLAR_ID.value, SUBGERAL.MILOSKI_ID.value],
  tags=["CIT", "SUBGERAL"],
  on_crashed=[clear_sisreg_limit],
  on_cancellation=[clear_sisreg_limit],
)
def extract_sisreg_api(
  es_index: Literal[
    "solicitacao-ambulatorial-rj", "marcacao-ambulatorial-rj", "solicitacao-hospitalar-rj"
  ],
  data_inicio: Optional[str] = None,
  data_fim: Optional[str] = None,
  page_size: int = 10_000,
  dias_por_faixa: int = 1,
  dataset_id: str = "brutos_sisreg_api_v2",
  table_id: Optional[str] = None,
  environment: str = "dev",
):
  """
  Args:
    es_index(["solicitacao-ambulatorial-rj", "marcacao-ambulatorial-rj", "solicitacao-hospitalar-rj"]):
      Endpoint do ElasticSearch a ser contactado. No momento,
      a API só aceita 3 valores possíveis.
    data_inicio(str?):
      Data, no formato ISO (YYYY-MM-DD), a partir da qual
      registros são obtidos. Quando None, é `data_fim` - 6 meses.
    data_fim(str?):
      Data, no formato ISO (YYYY-MM-DD), até a qual
      registros são obtidos. Quando None, é o dia de hoje.
    page_size(int?):
      As respostas da API são paginadas; esse é o limite de
      registros por página. Por padrão, 10,000, o máximo que
      a API permite.
    dias_por_faixa(int?):
      Quantos dias cada task deve processar
    dataset_id(str?):
      Nome do dataset onde os dados devem ser inseridos.
      Por padrão, 'brutos_sisreg_api'.
    table_id(str?):
      Nome da tabela onde os dados devem ser inseridos.
      Se None (padrão), é inferido de 'es_index': por exemplo,
      o endpoint "marcacao-ambulatorial-rj" vai para a tabela
      "marcacao_ambulatorial_rj".
    environment(str?):
      Ambiente de execução, "dev" (padrão) ou "prod".
  """
  # Guia:
  # https://servicos-datasus.saude.gov.br/detalhe/jDCFmnHyYQ
  # Manual:
  # https://mobileapps.saude.gov.br/portal-servicos/files/f3bd659c8c8ae3ee966e575fde27eb58/dfabfeedef07f675a142b63fa2553c6f_msv3sgc2e.pdf

  username = get_secret_task(
    secret_name="ES_USERNAME", environment=environment, path="/sisreg_api"
  )
  password = get_secret_task(
    secret_name="ES_PASSWORD", environment=environment, path="/sisreg_api"
  )

  faixas = gerar_faixas_de_data(
    data_inicio=data_inicio, data_fim=data_fim, dias_por_faixa=dias_por_faixa
  )

  # 1) Extrai e salva cada lote em disco, retorna dataframe
  extraction_futures = []
  for inicio, fim in faixas:
    rate_limit("meio-por-segundo")
    extraction_futures.append(
      extract_from_api.submit(
        user=username,
        password=password,
        index_name=es_index,
        page_size=page_size,
        data_inicio=inicio,
        data_fim=fim,
      )
    )
  wait(extraction_futures)
  dataframes = [future.result() for future in extraction_futures]

  # 2) Faz upload dos dataframes como tabelas
  # Sem .submit(), são uploads sequenciais/bloqueantes
  for df in dataframes:
    upload_df_to_datalake(
      df=df,
      dataset_id=dataset_id,
      table_id=(table_id if table_id else table_name_from_resource(es_index)),
      dump_mode="append",
      source_format="parquet",
      date_partition_column="data_particao",
    )
  # TODO: validação de quais uploads foram bem sucedidos, quais não


_flows = [flow_config(flow=extract_sisreg_api, schedules=schedules, memory="small")]
