# -*- coding: utf-8 -*-
from google.api_core.exceptions import BadRequest as GoogleBadRequest
from google.cloud import bigquery
from pandas import DataFrame

from pipelines.utils.datalake import upload_df_to_datalake
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def clone_bigquery_table(
  source_project_name: str,
  source_dataset_name: str,
  source_table_list: list[str],
  destination_project_name: str,
  destination_dataset_name: str,
):
  """
  Clona tabelas de um projeto para outro no BigQuery. Usado geralmente para
  clonar tabelas de projetos externos à SMS. Cria o dataset de destino caso
  não exista. Em caso de erro no comando de clonagem, tenta copiar dados
  diretamente via `CREATE TABLE ... AS SELECT *`.

  Args:
    source_project_name (str):
      O nome do projeto fonte no BigQuery (ex. "rj-smfp", "datario", ...).
    source_dataset_name (str):
      Nome do dataset fonte no BigQuery.
    source_table_list (list[str]):
      Lista de nomes de tabelas a serem clonadas do dataset fonte.
    destination_project_name (str):
      Nome do projeto destino no BigQuery (ex. "rj-sms").
    destination_dataset_name (str):
      Nome do dataset destino no BigQuery.
  """
  bq_client = bigquery.Client()

  for table in source_table_list:
    source_table_id = f"{source_project_name}.{source_dataset_name}.{table}"
    destination_dataset_id = f"{destination_project_name}.{destination_dataset_name}"

    if destination_dataset_name == source_dataset_name:
      destination_table_id = f"{destination_dataset_id}.{table}_cloned"
    else:
      destination_table_id = f"{destination_dataset_id}.{table}"

    bq_client.create_dataset(destination_dataset_id, exists_ok=True)

    log(f"Clonando tabela '{source_table_id}' para '{destination_table_id}'")

    try:
      command = (
        f"DROP TABLE IF EXISTS `{destination_table_id}`;\n"
        f"CREATE OR REPLACE TABLE `{destination_table_id}` CLONE `{source_table_id}`;"
      )
      log(f"Executando comando:\n\t{command}")
      query_job = bq_client.query_and_wait(command)
      job = bq_client.get_job(query_job.job_id)
      log(f"Resultado: {job.state}")

    except GoogleBadRequest as e:
      log(f"Clonagem automática falhou! Erro: {e}", level="warning")

      log("Tentando copiar dados diretamente")

      command = f"DROP TABLE IF EXISTS `{destination_table_id}`;"
      log(f"Executando comando:\n\t{command}")
      query_job = bq_client.query_and_wait(command)
      job = bq_client.get_job(query_job.job_id)
      log(f"Resultado: {job.state}")

      command = (
        f"CREATE TABLE `{destination_table_id}` AS SELECT * FROM `{source_table_id}`"
      )
      log(f"Executando comando:\n\t{command}")
      query_job = bq_client.query_and_wait(command)
      job = bq_client.get_job(query_job.job_id)
      log(f"Resultado: {job.state}")


@task
def download_then_reupload_bigquery_table(
  source_project_name: str,
  source_dataset_name: str,
  source_table_name: str,
  destination_dataset_name: str,
  chunk_size: int,
):
  """
  Clona uma tabela BigQuery para outra de forma ineficiente; faz `SELECT *`
  em pedaços da tabela fonte, depois faz re-upload dos dataframes resultantes
  como uma nova tabela, como parquet.

  Args:
    source_project_name (str):
      O nome do projeto fonte no BigQuery (ex. "rj-smfp", "datario", ...).
    source_dataset_name (str):
      Nome do dataset fonte no BigQuery.
    source_table_name (str):
      Nome da tabela a ser clonada do dataset fonte.
    destination_dataset_name (str):
      Nome do dataset destino no BigQuery.
    chunk_size (int):
      Tamanho de cada pedaço a ser extraído da tabela (para não estourar
      a memória). Executa `SELECT * FROM ... LIMIT {chunk_size} OFFSET ...`.
  """
  bq_client = bigquery.Client()

  offset = 0
  while True:
    command = f"""
    SELECT *
    FROM `{source_project_name}.{source_dataset_name}.{source_table_name}`
    LIMIT {chunk_size} OFFSET {offset};
    """
    log(f"Executando comando:\n\t{command}")
    df: DataFrame = bq_client.query_and_wait(command).to_dataframe()

    upload_df_to_datalake(
      df=df,
      dataset_id=destination_dataset_name,
      table_id=source_table_name,
      # apaga tabela original no primeiro pedaço da extração
      dump_mode=("replace" if offset == 0 else "append"),
      source_format="parquet",
    )
    # Confere se já consumimos a tabela inteira
    if len(df) < chunk_size:
      break
    offset += chunk_size
