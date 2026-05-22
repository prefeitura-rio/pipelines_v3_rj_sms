# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import (
  fetch_case_page,
  get_latest_vote,
  scrape_case_info_from_page,
  upload_results,
)


@flow(
  name="Extração: TCM (Tribunal de Contas do Município)",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.AVELLAR_ID.value],
)
def extract_tribunal_de_contas_rj(
  case_id: str, dataset_id: str = "brutos_diario_oficial", environment: str = "dev"
):
  # Caso principal
  (case_obj, html) = fetch_case_page(case_num=case_id, env=environment)
  result = scrape_case_info_from_page(case_tuple=(case_obj, html))
  # Tenta encontrar votos no caso
  latest_vote = get_latest_vote(ctid=case_obj["_ctid"])
  # Agrupamento de resultados e upload
  upload_results(main_result=result, latest_vote=latest_vote, dataset=dataset_id)


_flows = [flow_config(flow=extract_tribunal_de_contas_rj, schedules=[])]
