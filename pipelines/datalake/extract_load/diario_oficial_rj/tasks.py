# -*- coding: utf-8 -*-
import urllib.parse
from typing import List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.datetime import now, parse_date_or_today

from .utils import (
  get_links_for_path,
  node_cleanup,
  parse_do_contents,
  report_extraction_status,
  send_get_request,
)


@task(retries=3, retry_delay_seconds=10*60)
def get_current_DO_identifiers(date: Optional[str], env: Optional[str]) -> List[str]:
  date = parse_date_or_today(date).strftime("%Y-%m-%d")

  # Precisamos pegar o identificador do DO do dia de hoje
  # Para isso, fazemos uma busca na API no formato:
  # https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/di:2025-06-02/df:2025-06-02/?q=%22rio%22

  BASE = "https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0"
  DATE_INTERVAL = f"/di:{date}/df:{date}"
  # Precisamos de algum texto de busca, então buscamos por "rio", entre aspas
  QUERY = "/?q=" + urllib.parse.quote('"rio"')
  URL = f"{BASE}{DATE_INTERVAL}{QUERY}"
  log(f"Obtendo DO para data '{date}'")

  # Faz requisição GET, recebe um JSON ou um Exception
  json = send_get_request(URL, "json")
  # Confere se houve erro na requisição
  if isinstance(json, Exception):
    # Se sim, reporta status de falha na extração para essa data
    report_extraction_status(False, date, environment=env)
    # Dá erro; task vai ser retentada daqui a N minutos
    raise json

  # Resposta é um JSON com todas as instâncias encontradas da busca
  # Porém temos campos de metadadaos que indicam quantas instâncias foram encontradas
  # em cada edição, e isso nos dá os IDs que queremos:
  distinct_ids = set()
  for edition in json["aggregations"]["Edicoes"]["buckets"]:
    distinct_ids.add(edition["key"])
  distinct_ids = list(distinct_ids)
  do_count = len(distinct_ids)
  log(f"Encontrado(s) {do_count} DO(s) distinto(s) na data especificada: {distinct_ids}")
  # Confere se não recebemos nenhum DO para a data atual
  if do_count <= 0:
    # Nesse caso, reporta status de falha na extração para essa data
    report_extraction_status(False, date, environment=env)
    # Dá erro; task vai ser retentada daqui a N minutos
    raise Exception("DO para data especificada não encontrado!")

  # Atrela os IDs à data atual
  result = list(zip(distinct_ids, [date] * do_count))
  return result


@task(retries=3, retry_delay_seconds=10*60)
def get_article_names_ids(diario_id_date: tuple) -> List[tuple]:
  assert len(diario_id_date) == 2, "Tupla deve ser par (id, date)!"
  diario_id = diario_id_date[0]

  URL = f"https://doweb.rio.rj.gov.br/portal/visualizacoes/view_html_diario/{diario_id}"
  log(f"Obtendo artigos para DO ID '{diario_id}'")
  # Faz requisição GET, recebe HTML
  html = send_get_request(URL, "html")
  # Confere se houve erro na requisição
  if isinstance(html, Exception):
    # Dá erro; task vai ser retentada daqui a N minutos
    raise html

  # Precisamos encontrar todas as instâncias relevantes de
  # <a class="linkMateria" identificador="..." pagina="" data-id="..." data-protocolo="..." data-materia-id="..."> # noqa
  # De onde queremos extrair `identificador` ou `data-materia-id`
  all_folders = html.find_all("span", attrs={"class": "folder"})
  results = []
  paths = [
    ["atos do prefeito", "decretos n"],
    ["secretaria municipal de saúde", "resoluções", "resolução n"],
    ["controladoria geral do município do rio de janeiro", "resoluções", "resolução n"],
    [
      "controladoria geral do município do rio de janeiro",
      "comissão de qualificação de organizações sociais",
    ],
    ["tribunal de contas do município", "resoluções", "resolução n"],
    ["tribunal de contas do município", "outros"],
    ["avisos editais e termos de contratos", "secretaria municipal de saúde", "avisos"],
    ["avisos editais e termos de contratos", "secretaria municipal de saúde", "outros"],
    [
      "avisos editais e termos de contratos",
      "controladoria geral do município do rio de janeiro",
      "outros",
    ],
    ["avisos editais e termos de contratos", "tribunal de contas do município", "outros"],
  ]
  for path in paths:
    results.extend(get_links_for_path(all_folders, path))

  # Não temos como garantir que ambos os atributos vão existir sempre;
  # então pegamos o valor do primeiro preenhcido que encontrarmos
  def get_any_attribute(tag, attr_list):
    for attr in attr_list:
      val = tag.attrs.get(attr)
      if val is None:
        continue
      val = val.strip()
      if len(val):
        return val
    return None

  def get_folder_path(tag: BeautifulSoup):
    path = []
    while True:
      tag = tag.parent
      if tag is None or not tag or tag.attrs.get("id") == "tree":
        break
      folder = tag.findChild("span", attrs={"class", "folder"}, recursive=False)
      if folder is None or not folder:
        continue
      path.append(folder.text.strip())
    return "/".join(reversed(path))

  # Cria lista de par (título, ID); título é guardado direto no banco
  # e ID é usado pra pegar o conteúdo textual/HTML do artigo
  filtered_results = list(
    set(
      (
        get_folder_path(tag),
        tag.text.strip(),
        get_any_attribute(tag, ["identificador", "data-materia-id"]),
      )
      for tag in results
    )
  )
  log(f"Encontrado(s) {len(filtered_results)} artigo(s) relevante(s)")
  return [(diario_id_date, result) for result in filtered_results]


@task(retries=3, retry_delay_seconds=30)
def get_article_contents(do_tuple: tuple) -> List[dict]:
  assert len(do_tuple) == 2, "Tupla deve ser par ((do_id, date), (title, id))!"

  # Caso contrário, pega dados da etapa anterior
  (do_id, do_date) = do_tuple[0]
  (folder_path, title, id) = do_tuple[1]

  log(f"Obtendo conteúdo do artigo '{title}' (id '{id}')...")
  URL = f"https://doweb.rio.rj.gov.br/apifront/portal/edicoes/publicacoes_ver_conteudo/{id}"
  # Faz requisição GET, recebe HTML
  html = send_get_request(URL, "html")
  # Talvez o resultado não seja HTML (pode ser PDF por exemplo)
  if html is None:
    return None

  # Confere se houve erro na requisição
  if isinstance(html, Exception):
    # Se sim, retorna um valor que possamos detectar posteriormente
    return {"error": True}

  # Registra data/hora da extração
  base_result = {
    "_extracted_at": now(),
    "do_data": do_date,
    "do_id": do_id,
    "materia_id": id,
    "secao": folder_path,
    "titulo": title,
  }

  # Remove elementos inline comuns (<b>, <i>, <span>)
  clean_html = node_cleanup(html)
  # Faz parsing do conteúdo para deixar tudo estruturado
  content_list = parse_do_contents(clean_html.body)

  log(f"Artigo '{title}' (id '{id}') possui {len(content_list)} bloco(s)")
  ret = {
    **base_result,
    "sections": [
      {
        "secao_indice": section_index,
        "bloco_indice": block_index,
        "conteudo_indice": content_index,
        "cabecalho": content["header"],
        "conteudo": body,
      }
      for section_index, content in enumerate(content_list)
      for block_index, block in enumerate(content["body"])
      for content_index, body in enumerate(block)
    ],
  }
  return ret


@task(retries=1, retry_delay_seconds=30)
def upload_results(results_list: List[dict], dataset: str, date: Optional[str], env: Optional[str]):
  if len(results_list) == 0:
    log("Nada para fazer upload; saindo")
    return

  main_df = pd.DataFrame()

  # Para cada resultado
  for result in results_list:
    # Pula resultados 'vazios' (i.e. PDFs ao invés de texto, etc)
    if result is None:
      continue
    # Confere se tivemos erro em alguma requisição
    if "error" in result:
      # Se sim, reporta status de falha na extração para essa data
      report_extraction_status(False, date, environment=env)
      # E aborta o upload dos resultados
      return

    # Para cada seção no resultado
    for section in result["sections"]:
      # Constrói objeto a ser upado com informações base
      # Remove `sections` (obviamente)
      prepped_section = {**{k: v for k, v in result.items() if k != "sections"}, **section}
      # Constrói DataFrame a partir do objeto
      single_df = pd.DataFrame.from_records([prepped_section])
      # Concatena com os outros resultados
      main_df = pd.concat([main_df, single_df], ignore_index=True)

  log(f"Fazendo upload de DataFrame: {len(main_df)} linha(s); colunas: {list(main_df.columns)}")
  # Chama a task de upload
  upload_df_to_datalake(
    df=main_df,
    dataset_id=dataset,
    table_id="diarios_municipio",
    date_partition_column="_extracted_at",
    source_format="csv",
    csv_delimiter=",",
  )

  # Se chegamos aqui, reporta status de sucesso na extração para essa data
  report_extraction_status(True, date, environment=env)
