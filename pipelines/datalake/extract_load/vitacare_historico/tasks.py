# -*- coding: utf-8 -*-
from google.cloud import bigquery
from google.cloud.bigquery import ScalarQueryParameter

from pipelines.utils.datetime import now
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants


@task
def get_vitacare_unit_info() -> list[dict]:
	query = """
		select distinct
			id_cnes as cnes,
			area_programatica as ap,
			nome_limpo
		from `rj-sms.saude_dados_mestres.estabelecimento`
		where prontuario_versao = 'vitacare'
		and prontuario_episodio_tem_dado = 'sim'
		and id_cnes is not null
	"""

	log("Buscando unidades Vitacare no BigQuery")
	try:
		client = bigquery.Client()
		rows = client.query(query).result()

		result = [
			{
				"cnes": row.cnes,
				"ap": row.ap,
				"unit_name": row.nome_limpo,
			}
			for row in rows
		]

		log(f"Encontradas {len(result)} unidades esperadas no BigQuery")
		return result

	except Exception as exc:
		log(f"Erro ao buscar unidades esperadas no BigQuery: {exc}", level="error")
		raise


@task
def get_depara_gdrive() -> list[dict]:
	query = """
		select 
			unidade_nome_gdrive,
			cnes,
			unidade_nome
		from `rj-sms-dev.vitacare_controle.depara_gdrive`
	"""

	log("Buscando depara gdrive")
	try:
		client = bigquery.Client()
		rows = client.query(query).result()

		result = [
			{
				"nome_gdrive": row.unidade_nome_gdrive,
				"cnes": row.cnes,
				"unit_name": row.unidade_nome,
			}
			for row in rows
		]

		log(f"Encontradas {len(result)} unidades")
		return result

	except Exception as exc:
		log(f"Erro ao buscar unidades: {exc}", level="error")
		raise


@task
def populate_pipeline_status_table(
	expected_units: list[dict], reference_month
) -> None:
	client = bigquery.Client()
	dataset_id = constants.PIPELINE_STATUS_DATASET_ID.value
	table_id = constants.PIPELINE_STATUS_TABLE_ID.value
	table_ref = client.dataset(dataset_id).table(table_id)

	if hasattr(reference_month, "date"):
		reference_month = reference_month.date()

	log(
		f"Semeando pipeline_status para mês de referência '{reference_month}' "
		f"com {len(expected_units)} unidade(s) esperada(s)"
	)

	existing_query = f"""
		select cnes
		from `rj-sms-dev.vitacare_controle.pipeline_status`
		where reference_month = @reference_month
	"""
	existing_job_config = bigquery.QueryJobConfig(
		query_parameters=[
			ScalarQueryParameter("reference_month", "DATE", reference_month)
		]
	)
	existing_rows = client.query(
		existing_query,
		job_config=existing_job_config,
	).result()
	existing_cnes = {str(row.cnes) for row in existing_rows}

	current_timestamp = now().isoformat()
	rows_to_insert = []
	for unit in expected_units:
		cnes = unit.get("cnes")
		if cnes is None or str(cnes) in existing_cnes:
			continue

		rows_to_insert.append(
			{
				"reference_month": reference_month.isoformat(),
				"cnes": str(cnes),
				"ap": unit.get("ap"),
				"unit_name": unit.get("unit_name"),
				"source_folder_name": None,
				"source_file_name": None,
				"gdrive_to_gcs_status": "pending",
				"gdrive_to_gcs_error": None,
				"gcs_to_cloudsql_status": "pending",
				"gcs_to_cloudsql_error": None,
				"cloudsql_to_bigquery_status": "pending",
				"cloudsql_to_bigquery_error": None,
				"created_at": current_timestamp,
				"updated_at": current_timestamp,
			}
		)

	if not rows_to_insert:
		log("Nenhum novo registro precisa ser inserido em pipeline_status")
		return

	errors = client.insert_rows_json(table_ref, rows_to_insert)
	if errors:
		log(f"Erro inserindo linhas em pipeline_status: {errors}", level="error")
		raise RuntimeError(f"Erro inserindo linhas em pipeline_status: {errors}")

	log(f"Inseridas {len(rows_to_insert)} nova(s) linha(s) em pipeline_status")
