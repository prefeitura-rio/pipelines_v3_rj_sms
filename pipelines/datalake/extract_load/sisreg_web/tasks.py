# -*- coding: utf-8 -*-
from hashlib import sha256
import os

import pandas as pd
import requests

from pipelines.utils.cleanup import cleanup_columns_for_bigquery, prettify_byte_size
from pipelines.utils.datetime import now_str
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants


@task
def login(username: str, password: str):
  URL = constants.URL.value
  # Por algum motivo insano eles fazem
  # toUpperCase() na senha ANTES do hash
  hashed_pw = sha256(password.upper().encode()).hexdigest()

  res = requests.post(
    URL,
    headers={
      "referer": URL,
      "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/148.0.0.0 Safari/537.36"
      ),
    },
    data={
      "usuario": username,
      # Form de login do site apaga o campo senha
      # e preenche o campo senha_256 com o hash
      "senha": "",
      "senha_256": hashed_pw,
      "etapa": "ACESSO",
      "logout": "",
    },
    allow_redirects=False,
  )
  # Aqui devemos ter recebido uma resposta HTTP 302
  # redirecionando pra /cgi-bin/index
  res.raise_for_status()
  log(f"Login retornou status {res.status_code}")
  redirect = res.headers.get("location")
  if not redirect:
    log("Cabeçalho de redirecionamento não foi recebido!", level="warning")
  cookies_dict = res.cookies.get_dict()
  return cookies_dict

@task
def baixar_endpoint(endpoint: str, cookies: dict):
  URL = constants.URL.value
  config = constants.ENDPOINTS.value.get(endpoint)
  if config is None:
    raise ValueError(f"Endpoint '{endpoint}' não está configurado!")

  endpoint_url = config["endpoint"]
  endpoint_params = config["params"]

  folder_path = create_tmp_data_folder(prefix=f"sisreg-{endpoint}")
  file_path = os.path.join(folder_path, f"{endpoint}")

  log("Baixando arquivo...")
  with requests.get(
    url=f"{URL}{endpoint_url}",
    params=endpoint_params,
    stream=True
  ) as res:
    res.raise_for_status()
    with open(file_path, "wb") as file:
      for chunk in res.iter_content(chunk_size=8192):
        file.write(chunk)
  log(f"Arquivo baixado para '{file_path}'")
  file_size = prettify_byte_size(os.path.getsize(file_path))
  log(f"Arquivo possui tamanho {file_size}")
  return file_path


@task
def file_to_dataframe(file_path: str):
  df = pd.read_csv(file_path, sep=";")
  df = cleanup_columns_for_bigquery(df)
  df = df.drop_duplicates(ignore_index=True)
  df["_extracted_at"] = now_str()
  log(f">>>> Formato do DataFrame: {df.shape}")
  log(f">>>> Colunas: {df.columns}")
  return df
