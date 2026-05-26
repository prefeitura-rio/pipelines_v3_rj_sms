# -*- coding: utf-8 -*-
from typing import List, Optional

from prefect.futures import PrefectFutureList

from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import (
  get_article_contents,
  get_article_names_ids,
  get_current_DO_identifiers,
  upload_results,
)


@flow(
  name="Extração: DO-RJ (Diário Oficial Municipal)",
  description="Extrai dados relevantes à SMS do Diário Oficial Municipal",
  owners=[CIT.AVELLAR_ID.value],
  tags=["CIT"],
)
def extract_diario_oficial_rj(
  date: Optional[str] = None,
  dataset_id: str = "brutos_diario_oficial",
  environment: str = "dev",
):
  # Podemos ter múltiplos DOs em um dia...
  diario_ids = get_current_DO_identifiers(date=date, env=environment)
  # Para cada DO, pegamos todos os artigos...
  do_article_tuple: PrefectFutureList = get_article_names_ids.map(
    diario_id_date=diario_ids
  )
  # No Prefect v1, tínhamos uma função `flatten()`;
  # no v3 não mais, então fazemos na mão
  # TODO: não é possível que isso seja a melhor forma de fazer
  article_tuples: List[tuple] = []
  for tup_list_future in do_article_tuple:
    tup_list: List[tuple] = tup_list_future.result()
    for tup in tup_list:
      article_tuples.append(tup)
  # Para cada par de nome/id, pega o conteúdo do artigo
  article_contents = get_article_contents.map(do_tuple=article_tuples)
  # Upload dos resultados no final :)
  upload_results(
    results_list=article_contents, dataset=dataset_id, date=date, env=environment
  )


_flows = [
  flow_config(
    flow=extract_diario_oficial_rj,
    schedules=[],  # flow sem schedule, será chamado pelo orquestrador
    memory="small",
  )
]
