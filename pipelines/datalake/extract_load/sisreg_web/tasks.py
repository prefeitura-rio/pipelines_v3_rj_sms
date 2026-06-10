# -*- coding: utf-8 -*-
import os
from hashlib import sha256

import pandas as pd
import requests

from pipelines.utils.cleanup import cleanup_columns_for_bigquery, prettify_byte_size
from pipelines.utils.datetime import now_str, today
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants


@task
def login(username: str, password: str):
  URL = constants.URL.value
  USER_AGENT = constants.USER_AGENT.value
  # Por algum motivo insano eles fazem
  # toUpperCase() na senha ANTES do hash
  hashed_pw = sha256(password.upper().encode()).hexdigest()

  res = requests.post(
    URL,
    headers={"referer": URL, "user-agent": USER_AGENT},
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
  log(
    f"Login retornou status {res.status_code} "
    f"({'bom sinal' if res.status_code == 302 else 'possível problema'})"
  )
  redirect = res.headers.get("location")
  if not redirect:
    log("Cabeçalho de redirecionamento não foi recebido!", level="warning")
  cookies_dict = res.cookies.get_dict()
  if "SESSION" not in cookies_dict:
    log(
      f"Cookie de sessão não foi obtido! Provável erro iminente\n{cookies_dict}",
      level="warning",
    )
  return cookies_dict


@task(retries=3, retry_delay_seconds=5 * 60)
def baixar_endpoint(endpoint: str, cookies: dict):
  URL = constants.URL.value
  USER_AGENT = constants.USER_AGENT.value

  config = constants.ENDPOINTS.value.get(endpoint)
  if config is None:
    raise ValueError(f"Endpoint '{endpoint}' não está configurado!")

  endpoint_url = config["endpoint"]
  endpoint_params = config["params"]

  folder_path = create_tmp_data_folder(prefix=f"sisreg-{endpoint}")
  file_path = os.path.join(folder_path, f"{endpoint}")

  log("Baixando arquivo... (costuma levar ~1min)")
  with requests.get(
    url=f"{URL}{endpoint_url}",
    headers={"referer": URL, "user-agent": USER_AGENT},
    params=endpoint_params,
    cookies=cookies,
    stream=True,
  ) as res:
    res.raise_for_status()
    with open(file_path, "wb") as file:
      for chunk in res.iter_content(chunk_size=8192):
        file.write(chunk)
  log(f"Arquivo baixado para '{file_path}'")
  file_size = os.path.getsize(file_path)
  if file_size <= 0:
    log("Arquivo veio vazio! Retentando em alguns minutos...", level="error")
    raise RuntimeError()
  pretty_file_size = prettify_byte_size(file_size)
  log(f"Arquivo possui tamanho {pretty_file_size}")
  return file_path


@task
def file_to_dataframe(file_path: str):
  df = pd.read_csv(file_path, sep=";")
  df = cleanup_columns_for_bigquery(df, lowercase=True)
  # historicamente, colunas nessa tabela têm '__' trocado por '_'
  df.columns = df.columns.str.replace("__", "_")
  df = df.drop_duplicates(ignore_index=True)
  df["data_particao"] = today()
  df["_data_carga"] = now_str()
  log(f">>>> Formato do DataFrame: {df.shape}")
  log(f">>>> Colunas: {df.columns}")
  # Apaga arquivo CSV original, não precisamos dele em memória
  # agora que já temos o DataFrame
  os.remove(file_path)
  return df
