# -*- coding: utf-8 -*-
import time
from datetime import datetime, timedelta
from typing import List, Optional
from uuid import uuid4

import pandas as pd

from pipelines.utils.cleanup import cleanup_columns_for_bigquery
from pipelines.utils.datetime import (
  is_valid_YYYYMMDD,
  now_str,
  parse_date_or_today,
  today_str,
)
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants as flow_constants
from .utils import build_ES_query, connect_ES


@task
def gerar_faixas_de_data(
  data_inicio: Optional[str] = None,
  data_fim: Optional[str] = None,
  dias_por_faixa: int = 1,
):
  """
  Gera uma lista de tuplas (inicio, fim) dividindo o intervalo
  entre data_inicial e data_final em blocos de tamanho 'dias_por_faixa'.
  * Caso `data_inicio` seja None, será `data_fim` subtraída de 6 meses.
  * Caso `data_fim` seja None, será o dia de hoje.
  """
  dt_fim = parse_date_or_today(data_fim).date()

  if not data_inicio:
    dt_inicio = dt_fim - timedelta(days=6 * 30)
  else:
    dt_inicio = datetime.fromisoformat(data_inicio).date()

  log("Gerando faixas de datas para processamento em lotes")
  faixas = []
  while dt_inicio <= dt_fim:
    # Calcula faixa
    dt_chunk_inicio = dt_inicio
    dt_chunk_fim = dt_chunk_inicio + timedelta(days=dias_por_faixa - 1)
    # Se a faixa termina depois do limite, trunca
    if dt_chunk_fim > dt_fim:
      dt_chunk_fim = dt_fim
    # Salva faixa calculada
    faixa_inicio_str = dt_chunk_inicio.isoformat()
    faixa_fim_str = dt_chunk_fim.isoformat()
    faixas.append((faixa_inicio_str, faixa_fim_str))
    # Nova data de início
    dt_inicio = dt_chunk_fim + timedelta(days=1)

  log(f"{len(faixas)} faixas de datas geradas com sucesso.")
  return faixas


@task(retries=5, retry_delay_seconds=30)
def extract_from_api(
  user: str,
  password: str,
  index_name: str,
  page_size: int,
  data_inicio: str,
  data_fim: str,
):
  """
  Extrai dados do SISREG via API do ElasticSearch,
  considerando apenas o intervalo [data_inicial, data_final].

  Ao final, escreve em disco em formato Parquet e
  retorna apenas o caminho do arquivo.
  """
  # Valida as datas recebidas
  if not is_valid_YYYYMMDD(data_inicio):
    raise ValueError(f"Data inicial '{data_inicio}' é inválida!")

  if not is_valid_YYYYMMDD(data_fim):
    raise ValueError(f"Data final '{data_fim}' é inválida!")

  if data_inicio > data_fim:
    raise ValueError(
      f"Data inicial '{data_inicio}' não pode ser posterior à data final '{data_fim}'!"
    )

  extracted_at = now_str()

  ###

  es = connect_ES(flow_constants.API_URL.value, user, password)
  query = build_ES_query(page_size, data_inicio, data_fim)

  ####
  ####  Primeira consulta
  ####

  retries = 0
  max_retries = 5
  while True:
    # Consulta API
    # TODO: migrar para paginação com "search_after" ao invés de scrolls
    resposta: dict = es.search(
      index=index_name, body=query, scroll=flow_constants.SCROLL_TIMEOUT.value
    )
    # Se conseguiu obter os dados, sai
    if not resposta.get("timed_out", False):
      break
    # Caso contrário, retenta até `max_retries` vezes
    retries += 1
    if retries > max_retries:
      raise RuntimeError("Timeout repetido na consulta inicial.")

    log(f"({retries}/{max_retries}) Timeout na consulta; retentando em 10s")
    time.sleep(10)

  # Confere metadados
  # '_shards': {'total': x, 'successful': x, 'skipped': x, 'failed': x}
  shards: dict = resposta.get("_shards", {})
  if shards.get("failed", 0) > 0 or shards.get("skipped", 0) > 0:
    raise RuntimeError(f"Consulta com falhas em shards: {shards}")

  # Identificador do estado dos dados quando a requisição foi feita
  # É necessário para que dados de páginas seguintes sejam consistentes
  # e não tenham sofrido alterações entre requisições
  scroll_id: str = resposta["_scroll_id"]  # é um ID de ~1.5kB :s
  # 'hits': {
  #   'total': {'value': x, 'relation': 'eq'},
  #   'max_score': x,
  #   'hits': [ ... ]   # Dados de verdade estão aqui
  # }
  total_obj: dict = resposta["hits"]["total"]
  total_registros = total_obj["value"] if total_obj.get("relation") == "eq" else None

  hits: List[dict] = resposta["hits"]["hits"]
  if total_registros == 0 or not hits:
    log(f"Nenhum registro no intervalo {data_inicio} a {data_fim}.")
    return None
  log(f"Total de registros encontrados ({data_inicio} a {data_fim}): {total_registros}")

  # 'hits': [
  #   {
  #     '_index': 'xxx',  # Nome do endpoint; ex. 'solicitacao-ambulatorial'
  #     '_type': '_doc',
  #     '_id': 'xxx',
  #     '_score': x,
  #     '_source': {
  #       ... # Dados de verdade (agora é sério)
  #     },
  #     "sort": [ x, 'xxx' ]
  #   },
  #   ...
  # ]
  # Dados de verdade ficam no '_source', é um dicionário enorme
  dados: List[dict] = [reg.get("_source", {}) for reg in hits]
  log(f"Processados {len(dados)}/{total_registros} registros (lote inicial)")

  ####
  ####  Próximas páginas
  ####

  scroll_ids = [scroll_id]
  while True:
    resposta = es.scroll(scroll_id=scroll_id, scroll=flow_constants.SCROLL_TIMEOUT.value)

    # Atualiza o scroll_id caso ele seja novo
    new_scroll_id = resposta.get("_scroll_id")
    if new_scroll_id and scroll_id != new_scroll_id:
      scroll_ids.append(new_scroll_id)
      scroll_id = new_scroll_id

    # '_shards': {'total': x, 'successful': x, 'skipped': x, 'failed': x}
    shards: dict = resposta.get("_shards", {})
    if shards.get("failed", 0) > 0 or shards.get("skipped", 0) > 0:
      raise RuntimeError(f"Busca inicial com falhas em shards: {shards}")

    hits = resposta["hits"]["hits"]
    if not hits:
      break

    dados.extend([reg.get("_source", {}) for reg in hits])
    log(f"Processados {len(dados)}/{total_registros} registros")

  # Scrolls em aberto consomem memória do servidor de API;
  # limpa os scrolls porque somos educados
  es.options(ignore_status=(404,)).clear_scroll(scroll_id=scroll_ids)

  # Valida dados recebidos vs. reportados
  total_obtido = len(dados)
  diff = abs(total_obtido - total_registros)
  # Permite até 5% de diferença
  if total_registros > 0 and abs(diff / total_registros) > 0.05:
    raise ValueError(
      f"Divergência na contagem de registros processados. "
      f"Esperado: {total_registros}, Obtido: {total_obtido}"
    )

  df = pd.DataFrame(dados, dtype=str)
  df["_run_id"] = str(uuid4())
  df["_extracted_at"] = extracted_at
  df["data_extracao"] = today_str()
  df = cleanup_columns_for_bigquery(df, lowercase=True)
  return df


# @task()
# def validate_upload(
#   run_id,
#   as_of,
#   environment,
#   bq_table,
#   bq_dataset,
#   data_inicio,
#   data_fim,
#   slice_completed,
# ):
#   values = slice_completed or []
#   total_slices = len(values)

#   # Slices que passaram por mark_slice_completed e retornaram True
#   succeeded_slices = sum(1 for v in values if v)
#   failed_slices = total_slices - succeeded_slices

#   # Completed apenas se todos os slices finalizaram bem
#   completed = failed_slices == 0

#   row = {
#     "run_id": run_id,
#     "as_of": as_of,
#     "environment": environment,
#     "bq_table": bq_table,
#     "bq_dataset": bq_dataset,
#     "data_inicio": data_inicio,
#     "data_fim": data_fim,
#     "validation_date": datetime.now(),
#     "completed": completed,
#   }
#   df = pd.DataFrame([row])
#   return df
