import base64

from pandas import DataFrame

from pipelines.utils.api import POST
from pipelines.utils.prefect import authenticated_task as task


@task
def auth(url: str, user: str, password: str):

  encoded_bytes = base64.b64encode(password.encode("utf-8"))
  password_b64 = encoded_bytes.decode("utf-8")

  body = {"user": user, "password": password_b64, "method": "getToken"}

  token = POST(url, json=body)

  return token


def get_fatos(url, cnes, tabela, data, access_token, token):
  body = {
    "cnes": cnes,
    "tabela": tabela,
    "data": data,
    "method": "getFatos",
    "access_token": access_token,
    "token": token,
  }

  r = POST(url, json=body)

  data_list = r.json()["data_list"]

  dados = DataFrame(data_list)

  return dados
