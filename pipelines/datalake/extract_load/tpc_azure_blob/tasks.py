# -*- coding: utf-8 -*-
import csv

import pandas as pd

from pipelines.utils.azure import download_azure_blob
from pipelines.utils.datetime import now_str, today_str
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants as tpc_constants


@task(retries=4, retry_delay_seconds=180)
def extract_data_from_blob(
  blob_file: str, file_folder: str, file_name: str, environment: str = "dev"
) -> str:
  """
  Extrai arquivo do Azure Blob Storage e valida se a data de replicação é atual.
  """
  container_name = tpc_constants.CONTAINER_NAME.value
  blob_path = tpc_constants.BLOB_PATH.value[blob_file]

  token = get_secret(
    secret_name=tpc_constants.INFISICAL_TPC_TOKEN.value,
    path=tpc_constants.INFISICAL_PATH.value,
    environment=environment,
  )

  file_path = download_azure_blob(
    container_name=container_name,
    blob_path=blob_path,
    file_folder=file_folder,
    file_name=file_name,
    credentials=token,
    add_load_date_to_filename=True,
  )

  log("Verificando se o arquivo é da data atual")
  df = pd.read_csv(
    file_path,
    sep=";",
    quoting=csv.QUOTE_MINIMAL,
    encoding="utf-8",
    quotechar='"',
    escapechar="\\",
    keep_default_na=False,
    dtype=str,
    nrows=1,
  )

  replication_date = pd.Timestamp(df.data_atualizacao[0]).strftime("%Y-%m-%d")
  log(f"Data de replicação do arquivo: {replication_date}")

  today = today_str()
  if replication_date != today:
    raise RuntimeError(
      f"Arquivo não é da data atual. Data de replicação: {replication_date}"
    )

  log("Arquivo é da data atual")
  return file_path


@task
def transform_data(file_path: str, blob_file: str) -> str:
  """
  Conforma o CSV baixado para carga no BigQuery.
  """
  log("Conformando CSV para o GCP")

  df = pd.read_csv(
    file_path,
    sep=";",
    quoting=csv.QUOTE_MINIMAL,
    encoding="utf-8",
    quotechar='"',
    escapechar="\\",
    keep_default_na=False,
    dtype=str,
  )

  if blob_file == "posicao":
    df = df[df.sku != ""]

    df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
    df["peso_bruto"] = df.peso_bruto.apply(lambda x: float(x.replace(",", ".")))
    df["qtd_dispo"] = df.qtd_dispo.apply(lambda x: float(x.replace(",", ".")))
    df["qtd_roma"] = df.qtd_roma.apply(lambda x: float(x.replace(",", ".")))
    df["preco_unitario"] = df.preco_unitario.apply(
      lambda x: float(x.replace(",", ".")) if x != "" else x
    )

    df["validade"] = df.validade.apply(lambda x: x[:10])
    df["dt_situacao"] = df.dt_situacao.apply(
      lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2]
    )
  elif blob_file == "pedidos":
    df["valor"] = df.valor.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
    df["peso"] = df.peso.apply(lambda x: float(x.replace(",", ".")))
    df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
    df["quantidade_peca"] = df.quantidade_peca.apply(lambda x: float(x.replace(",", ".")))
    df["valor_total"] = df.valor_total.apply(
      lambda x: float(x.replace(",", ".")) if x != "" else x
    )
  elif blob_file == "recebimento":
    df["qt"] = df.qt.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
    df["qt_fis"] = df.qt_fis.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
    df["pr_unit"] = df.pr_unit.apply(
      lambda x: float(x.replace(",", ".")) if x != "" else x
    )
    df["vl_merc"] = df.vl_merc.apply(
      lambda x: float(x.replace(",", ".")) if x != "" else x
    )
    df["vl_total"] = df.vl_total.apply(
      lambda x: float(x.replace(",", ".")) if x != "" else x
    )
    df["qt_rec"] = df.qt_rec.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)

  df["_data_carga"] = now_str()

  df.to_csv(file_path, index=False, sep=";", encoding="utf-8", quoting=0, decimal=".")

  log("CSV conformado")
  return file_path
