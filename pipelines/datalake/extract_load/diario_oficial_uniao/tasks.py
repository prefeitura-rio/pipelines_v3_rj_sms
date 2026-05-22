# -*- coding: utf-8 -*-
import datetime
import os
import shutil
import zipfile

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_uniao.constants import (
  constants as flow_constants,
)
from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.datetime import now, now_str, parse_date_or_today
from pipelines.utils.infisical import get_secret
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def create_dirs() -> dict[str, str]:
  download_dir = create_tmp_data_folder(prefix="download")
  output_dir = create_tmp_data_folder(prefix="output")
  return {"download_dir": download_dir, "output_dir": output_dir}


@task
def parse_date(date: str) -> datetime.datetime:
  return parse_date_or_today(date)


@task(retries=3, retry_delay_seconds=600)
def login(enviroment: str = "dev"):
  password = get_secret(
    path=flow_constants.INFISICAL_PATH.value,
    secret_name=flow_constants.INFISICAL_PASSWORD.value,
    environment=enviroment,
  )
  email = get_secret(
    path=flow_constants.INFISICAL_PATH.value,
    secret_name=flow_constants.INFISICAL_USERNAME.value,
    environment=enviroment,
  )

  """Inicia uma sessão e faz o login no sistema da INLABS para obter os cookies.

    Returns:
        requests.Session: Instância de Session da biblioteca requests contém os cookies da sessão.
    """
  login_url = flow_constants.LOGIN_URL.value

  payload = {"email": email, "password": password}

  log("😚 Tentando fazer login...")
  # Caso algum erro 5xx ocorra, retenta automaticamente 3 vezes
  session = requests.Session()
  retries = Retry(
    total=3,
    backoff_factor=15,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=False,
  )
  session.mount("https://", HTTPAdapter(max_retries=retries))
  try:
    response = session.request(
      "POST", login_url, data=payload, headers=flow_constants.HEADERS.value
    )
    response.raise_for_status()
  # Se, mesmo após as retentativas automáticas, ainda assim esteja recebendo erro,
  # o retry da @task entra em jogo, e o processo inteiro é retentado em ~20min
  except Exception as exc:
    session.close()
    log("⚠️ Erro de conexão. Tentando novamente...")
    raise exc

  # Se saímos do login sem cookie de sessão, dá erro para retentativa
  if not session.cookies.get("inlabs_session_cookie"):
    session.close()
    raise Exception("⚠️ Falha ao obter cookie. Tentando novamente...")

  return session


@task(retries=3, retry_delay_seconds=600)
def download_files(
  session: requests.Session,
  sections: str,
  date: datetime.datetime,
  download_dir: str = None,
) -> list | None:
  """Faz o download dos arquivos .zip com os atos oficiais de cada seção para um dia específico.

  Args:
      session (requests.Session): Instância de Session da biblioteca requests
          que contém os cookies da sessão.
      sections (str): Seções do DOU a serem extraídas (DO1, DO2 e DO3)
      date (datetime.datetime): Data do diário oficial a ser extraído.

  Returns:
      list | None: Lista contendo o caminho dos arquivos .zip baixados ou None.
  """
  download_base_url = flow_constants.DOWNLOAD_BASE_URL.value

  cookie = session.cookies.get("inlabs_session_cookie")

  date_to_extract = date.strftime("%Y-%m-%d")

  files = []

  log("⏳️ Realizando requisições ao INLABS...")
  for dou_section in sections.split(" "):
    final_url = (
      download_base_url
      + date_to_extract
      + "&dl="
      + date_to_extract
      + "-"
      + dou_section
      + ".zip"
    )
    headers = {"Cookie": "inlabs_session_cookie=" + cookie, "origem": "736372697074"}

    response = session.request("GET", final_url, headers=headers)

    if response.status_code == 200:
      file_name = date_to_extract + "-" + dou_section + ".zip"
      if download_dir is None:
        download_dir = flow_constants.DOWNLOAD_DIR.value
      file_path = os.path.join(download_dir, file_name)
      with open(file_path, "wb") as file:
        file.write(response.content)
        files.append(file_path)
      if os.path.getsize(file_path) < 1000:
        log(f"❌ Arquivo vazio: {file_name}")
        raise Exception("Arquivo vazio.")
    elif response.status_code == 404:
      log(f"❌ Arquivo não encontrado: {date_to_extract + '-' + dou_section + '.zip'}")
      raise Exception("Arquivo não encontrado.")
  log("✅ Requisições feitas com sucesso.")

  return files


@task
def unpack_zip(zip_files: list, output_path: str) -> None:
  """Descompacta os arquivos .zip baixados.

  Args:
      zip_files (list): Lista com os arquivos .zip baixados.
      output_path (str): Caminho para o diretório onde os arquivos .xml serão armazenados.
  """
  try:
    log("⬇️ Iniciando descompactação dos arquivos .zip")
    if zip_files:
      for file in zip_files:
        log(f"Extraindo arquivos de: {file}")
        with zipfile.ZipFile(file, "r") as zip_ref:
          zip_ref.extractall(output_path)
    return True
  except Exception:
    log("⚠️ Não há atos oficias para descompactar")
    return False


@task
def get_xml_files(xml_dir: str, output_dir: str = None) -> str:
  """Pega as informações dos xml de cada ato oficial.

  Args:
      xml_dir (str): Diretório onde os arquivos .xml estão armazenados.

  Returns:
      str: Caminho do arquivo parquet gerado após o processamento das informações.
  """
  log("📋️ Iniciando processamento dos arquivos .xml...")
  try:
    acts = []
    for file in os.listdir(xml_dir):
      if file.endswith(".xml"):
        try:
          with open(os.path.join(xml_dir, file), "r", encoding="utf-8") as f:
            xml_data = f.read()
            soup_xml = BeautifulSoup(xml_data, "xml")
            soup_html = BeautifulSoup(soup_xml.find("Texto").text, "html.parser")
            id = soup_xml.find("article")["id"]
            act_id = soup_xml.find("article")["idOficio"]
            pdf_page = soup_xml.find("article")["pdfPage"]
            edition = soup_xml.find("article")["editionNumber"]
            pubname = soup_xml.find("article")["pubName"]
            number_page = soup_xml.find("article")["numberPage"]
            pub_date = soup_xml.find("article")["pubDate"]
            art_category = soup_xml.find("article")["artCategory"]
            html = soup_html.prettify()
            text = "\n".join(p.get_text() for p in soup_html.find_all("p"))
            assina = soup_html.find_all(class_="assina")
            cargos = soup_html.find_all(class_="cargo")
            identifica = soup_xml.find_all("Identifica")
            text_title = soup_html.find_all(class_="identifica")

            extracted_data = {
              "title": " ".join([title.text for title in identifica]),
              "id": id,
              "act_id": act_id,
              "text_title": " ".join(title.get_text() for title in text_title),
              "published_at": pub_date,
              "agency": art_category,
              "text": text,
              "url": pdf_page,
              "number_page": number_page,
              "edition": edition,
              "section": pubname,
              "html": html,
              "signatures": "/".join([signature.text for signature in assina]),
              "role": " / ".join([cargo.text for cargo in cargos]),
            }
            acts.append(extracted_data)
        except Exception as e:
          log(f"⚠️ Erro ao processar o arquivo {file}: {e}")

    df = pd.DataFrame(acts)
    df["extracted_at"] = now_str()

    file_name = f"dou-extraction-{now_str()}.parquet"
    if output_dir is None:
      output_dir = xml_dir
    file_path = os.path.join(output_dir, file_name)

    if df.empty:
      return ""
    df.to_parquet(file_path, index=False)
    log(f"📁 Arquivo {file_name} salvo.")
    return file_path

  except Exception as e:
    log(f"Erro fatal em get_xml_files: {e}")
    return ""


@task
def upload_to_datalake(parquet_path: str, dataset: str):
  """Função para realizar a carga dos dados extraídos no datalake da SMS Rio.
  Args:
      parquet_path (str): Caminho para o arquivo parquet com os dados a serem carregados.
      dataset (str): Dataset do BigQuery onde os dados serão carregados.

  Returns:
      bool: True se o upload for bem-sucedido, False caso contrário.
  """
  if parquet_path == "":
    log("⚠️ Não há registros para enviar ao datalake.")
    return False

  df = pd.read_parquet(parquet_path)
  log(f"⬆️ Realizando upload de {len(df)} registros para o datalake...")

  if df.empty:
    log("⚠️ Não há registros para enviar ao datalake.")
    return False

  upload_df_to_datalake(
    df=df,
    dataset_id=dataset,
    table_id="diarios_uniao_api",
    date_partition_column="extracted_at",
    dump_mode="append",
    source_format="parquet",
  )

  log("✅ Upload para o datalake finalizado.")
  return True


@task
def report_extraction_status(status: bool, date: str, environment: str = "dev"):
  """
  Reporta o status da extração para o BigQuery.

  Args:
      status (bool): Indica se a extração foi bem-sucedida (True) ou não (False).
      date (str): Data da extração.
      environment (str, optional): Ambiente de execução (ex: "dev", "prod"). Padrão para "dev".
  """
  log("⬆️ Reportando status da extração...")
  date = parse_date_or_today(date).strftime("%Y-%m-%d")

  success = "true" if status else "false"
  current_datetime = now()
  tipo_diario = "dou-api"

  if environment is None:
    environment = "dev"

  PROJECT = constants.GOOGLE_CLOUD_PROJECT.value[environment]
  DATASET = "projeto_cdi"
  TABLE = "extracao_status"
  FULL_TABLE_NAME = f"`{PROJECT}.{DATASET}.{TABLE}`"

  log(
    f"Inserting into {FULL_TABLE_NAME} status of success={success} for date='{date}'..."
  )

  client = bigquery.Client()
  query_job = client.query(
    f"""
        INSERT INTO {FULL_TABLE_NAME} (
            data_publicacao, tipo_diario, extracao_sucesso, _updated_at
        )
        VALUES (
            '{date}', '{tipo_diario}', {success}, '{current_datetime}'
        )
    """
  )
  query_job.result()
  log("✅ Status da extração reportado!")
  return


@task
def delete_dirs(download_dir: str = None, output_dir: str = None):
  """
  Deleta os diretórios temporários.
  """
  log("🗑️ Deletando diretórios temporários...")
  if download_dir is None:
    download_dir = flow_constants.DOWNLOAD_DIR.value
  if output_dir is None:
    output_dir = flow_constants.OUTPUT_DIR.value

  if os.path.exists(download_dir):
    shutil.rmtree(download_dir)
  if os.path.exists(output_dir):
    shutil.rmtree(output_dir)
  log("✅ Diretórios temporários deletados.")
