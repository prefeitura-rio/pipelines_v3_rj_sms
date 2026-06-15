# -*- coding: utf-8 -*-

from google.api_core.exceptions import BadRequest as GoogleBadRequest
from google.cloud import bigquery

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
  Clona uma tabela BigQuery para outra. Usado geralmente para clonar tabelas
  de projetos externos à SMS. Cria o dataset de destino caso não exista.
  Em caso de erro no comando de clonagem, tenta copiar dados diretamente
  via `SELECT *`.

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
