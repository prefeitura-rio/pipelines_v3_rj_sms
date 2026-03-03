# -*- coding: utf-8 -*-
import asyncio
import json
from datetime import datetime

from prefect import State
from prefect.logging import get_run_logger
from prefect.exceptions import MissingContextError
import pytz
from discord import Embed
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipelines.utils.logger import log
from pipelines.utils.flow import Flow
from pipelines.utils.infisical import inject_bd_credentials

from pipelines.utils.monitor import get_environment, send_discord_embed


def handle_flow_state_change(flow: Flow, old_state: State, new_state: State):
	try:
		log("")
	except MissingContextError as e:
		print("[handle_flow_state_change] MissingContextError: Não há flow excutando (ainda, ou mais) para o logger")
		return

	log(f"STATE CHANGE: {old_state.name} -> {new_state.name}", level="warning")

	environment = get_environment()

	inject_bd_credentials(environment=environment)

	info = {
		"flow_name": flow.name,
		"flow_id": "TODO-flow_id",
		"flow_run_id": "TODO-flow_run_id",
		"flow_parameters": json.dumps({ "TODO": "flow_parameters" }),
		"state": type(new_state).__name__,
		"message": new_state.message,
		"occurrence": datetime.now(tz=pytz.timezone("America/Sao_Paulo")).isoformat(),
	}

	if new_state.is_failed() and environment == "prod" and len(flow.get_owners()) > 0:
		message = [
			" ".join([f"<@{owner}>" for owner in flow.get_owners()]),
			f"> Flow Run: [TODO{'flow_run_name'}](https://pipelines.dados.rio/flow-run/{info['flow_run_id']})",
			f"*Parâmetros:*",
		]
		for key, value in ({ "TODO": "flow_parameters" }).items():
			message.append(f"- {key}: `{value}`")

		asyncio.run(
			send_discord_embed(
				contents=[
					Embed(
						title=info["flow_name"],
						description="\n".join(message),
						color=15158332,
					)
				],
				monitor_slug="error",
			)
		)

	rows = [info]
	# ------------------------------------------------------------
	# Sending data to BigQuery
	# ------------------------------------------------------------
	project_id = "rj-sms-dev" if environment == "dev" else "rj-sms"
	dataset_id = "brutos_prefect_staging"
	table_id = "flow_state_change"

	client = bigquery.Client(project=project_id)

	# Create Dataset if it does not exist
	dataset_ref = client.dataset(dataset_id)
	try:
		client.get_dataset(dataset_ref)
	except NotFound:
		dataset = bigquery.Dataset(dataset_ref)
		client.create_dataset(dataset)
		log(f"Created dataset {dataset_id}")

	# Create Table if it does not exist
	table_ref = dataset_ref.table(table_id)
	try:
		client.get_table(table_ref)
	except NotFound:
		schema = [
			bigquery.SchemaField("flow_name", "STRING"),
			bigquery.SchemaField("flow_id", "STRING"),
			bigquery.SchemaField("flow_run_id", "STRING"),
			bigquery.SchemaField("flow_parameters", "STRING"),
			bigquery.SchemaField("state", "STRING"),
			bigquery.SchemaField("message", "STRING"),
			bigquery.SchemaField("occurrence", "TIMESTAMP"),
		]
		table = bigquery.Table(table_ref, schema=schema)
		client.create_table(table)
		log(f"Created table {table_id}")

	# Insert rows
	errors = client.insert_rows_json(table_ref, rows)

	if errors:
		log(f"Encountered errors while inserting rows: {errors}")
	else:
		log(f"Rows inserted successfully")

	return new_state
