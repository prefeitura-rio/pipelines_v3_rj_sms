import base64

import requests
from pandas import DataFrame

from pipelines.datalake.extract_load.sarah_padi.constants import (
  constants as padi_constants,
)
from pipelines.utils.datetime import from_relative_date, now_str, parse_date_or_today
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def parse_date(date_str: str):
  if not date_str:
    date = from_relative_date("D-1")
  else:
    date = parse_date_or_today(date_str)

  date_str = date.strftime("%d/%m/%Y")
  log(f"Data selecionada: {date_str}", level="info")
  return date_str


@task
def auth(url: str, user: str, password: str):

  encoded_bytes = base64.b64encode(password.encode("utf-8"))
  password_b64 = encoded_bytes.decode("utf-8")

  body = {"user": user, "password": password_b64, "method": "getToken"}
  response = requests.post(url, json=body)
  payload = response.json()
  token = payload.get("token")

  return token


@task
def get_fatos(
  url: str, cnes: str, tabela: int, data: str, access_token: str, token: str
) -> DataFrame:

  TABLES = padi_constants.TABLES.value
  tabela_id = TABLES.get(tabela)

  body = {
    "cnes": cnes,
    "tabela": tabela_id,
    "data": data,
    "method": "getFatos",
    "access_token": access_token,
    "token": token,
  }
  response = requests.post(url, json=body)

  if response.status_code == 200:
    payload = response.json()
    data_list = payload.get("data_list")
    df = DataFrame(data_list)
    df["extracted_at"] = now_str()
    log(
      f"Foram encontrados {df.shape[0]} registros para {tabela} em {data}", level="info"
    )
  else:
    log(f"Erro ao buscar dados: {response.status_code}", level="error")
    raise Exception(f"Erro ao buscar dados: {response.status_code}\n{response.json()}")

  return df
