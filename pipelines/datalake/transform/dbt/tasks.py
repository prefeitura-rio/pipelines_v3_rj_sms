# -*- coding: utf-8 -*-

from datetime import datetime
import os
import shutil
from zoneinfo import ZoneInfo

from dbt.cli.main import dbtRunner, dbtRunnerResult
from google.cloud import bigquery
import pandas as pd
from prefect.states import Failed

from pipelines.utils.api import convert_usd_to_brl
from pipelines.utils.cleanup import process_null_str
from pipelines.utils.datalake import upload_to_datalake
from pipelines.utils.datetime import now
from pipelines.utils.env import environment_is_valid, get_google_project_for_environment
from pipelines.utils.google import download_from_bucket, upload_to_cloud_storage
from pipelines.utils.logger import log

# from pipelines.utils.monitor import send_discord_message
from pipelines.utils.prefect import authenticated_task as task

# from .utils import log_to_file, process_dbt_logs
from .constants import constants as dbt_constants


@task
def execute_dbt(
	repository_path: str,
	command: str = "run",
	target: str = "dev",
	select: str = "",
	exclude: str = None,
	state: str = None,
	flag: str = None,
) -> dict:
	"""
	Executa um comando dbt com os parâmetros especificados.

	Args:
		repository_path (str):
			Caminho, na máquina local, para o repositório do dbt.
		command (str?):
			O comando dbt a ser executado; p.ex.: "run", "build", etc.
			Possui valor padrão de "run".
		target (str?):
			O target (`--target`) do dbt; p.ex.: "dev", "ci", "prod".
			Possui valor padrão de "dev".
		select (str?):
			Valor passado ao argumento `--select`. É vazio por padrão.
		exclude (str?):
			Valor passado ao argumento `--exclude`. É vazio por padrão.
		state (str?):
			Valor passado ao argumento `--state`. É vazio por padrão.
		flag (str?):
			Flags adicionais passadas ao comando.
	"""
	commands = command.split(" ")

	cli_args = commands + [
		"--profiles-dir",
		repository_path,
		"--project-dir",
		repository_path,
	]

	if command in ("build", "data_test", "run", "test"):
		cli_args.extend(["--target", target])

		if select:
			trimmed_select = select.strip()
			cli_args.extend(["--select", trimmed_select])
		if exclude:
			trimmed_exclude = exclude.strip()
			cli_args.extend(["--exclude", trimmed_exclude])
		if state:
			trimmed_state = state.strip()
			cli_args.extend(["--state", trimmed_state])
		if flag:
			trimmed_flag = flag.strip()
			cli_args.extend([trimmed_flag])

		log(f"Executando comando dbt: '{' '.join(cli_args)}'")

	dbt_runner = dbtRunner()
	start_time = now()
	running_result: dbtRunnerResult = dbt_runner.invoke(cli_args)
	end_time = now()
	execution_time = (end_time - start_time).total_seconds()

	log_path = os.path.join(repository_path, "logs", "dbt.log")

	if command not in ("deps") and not os.path.exists(log_path):
		# FIXME
		# send_message(
		# 	title="❌ Erro ao executar DBT",
		# 	message="Não foi possível encontrar o arquivo de logs.",
		# 	monitor_slug="dbt-runs",
		# )
		return Failed(message="DBT Run seems not successful. No logs found.")

	return {
		"command": " ".join(cli_args),
		"running_result": running_result,
		"execution_time": execution_time,
		"start_time": start_time,
		"end_time": end_time,
		"log_path": log_path,
	}


@task
def estimate_dbt_costs(execution_info: dict, environment: str) -> float:
	"""
	Estima custo de uma execução de comando dbt
	"""
	affected_datasets = []
	running_result: dbtRunnerResult = execution_info["running_result"]
	for command_result in running_result.result:
		affected_datasets.append(command_result.node.schema)

	# Converte data/hora de execução para UTC
	start_time: datetime = execution_info["start_time"]
	end_time: datetime = execution_info["end_time"]
	start_time = start_time.astimezone(tz=ZoneInfo("UTC"))
	end_time = end_time.astimezone(tz=ZoneInfo("UTC"))

	# Consulta BigQuery para obter custos
	query_string = '/* {"app": "dbt",%'
	project_id = get_google_project_for_environment(environment=environment)
	query = f"""
	SELECT
		destination_table.project_id as destination_project_id,
		destination_table.dataset_id as destination_dataset_id,
		destination_table.table_id as destination_table_id,
		CASE statement_type
			WHEN 'SCRIPT'
				THEN 0
			WHEN 'CREATE_MODEL'
				THEN 50 * 6.25 * (total_bytes_billed / 1024 / 1024 / 1024 / 1024)
			ELSE 6.25 * (total_bytes_billed / 1024 / 1024 / 1024 / 1024)
		END as cost_in_usd,
	FROM `{project_id}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
	WHERE
		query like '{query_string}' and
		creation_time >= '{start_time}' and
		creation_time <= '{end_time}'
	ORDER BY creation_time
	"""
	client = bigquery.Client()
	query_job = client.query(query)
	results: pd.DataFrame = query_job.result().to_dataframe()

	results = results[results["destination_dataset_id"].isin(affected_datasets)]
	total_usd_cost = results["cost_in_usd"].sum()
	total_brl_cost = convert_usd_to_brl(total_usd_cost)
	log(f"Custo calculado: US${total_usd_cost:.4f} ~ R${total_brl_cost:.4f}")
	return total_brl_cost


######  TODO: 🚧🚧🚧🚧🚧 em construção  ######
@task
def create_dbt_report(
	execution_info: dict, estimated_total_cost: float, repository_path: str
) -> None:
	"""
	Args:
		running_results (dbtRunnerResult): The results of running dbt commands.
		repository_path (str): The path to the repository.

	Raises:
		FAIL: If there are failures in the dbt commands.

	Returns:
			None
	"""
	# running_results: dbtRunnerResult = execution_info["running_result"]
	# log_path: str = execution_info["log_path"]

	# logs = process_dbt_logs(log_path=os.path.join(repository_path, "logs", "dbt.log"))
	# log_path = log_to_file(logs)
	# summarizer = Summarizer()

	# is_successful, has_warnings = True, False

	general_report = []
	# for command_result in running_results.result:
	# 	if command_result.status == "fail":
	# 		is_successful = False
	# 		general_report.append(f"- 🛑 FAIL: {summarizer(command_result)}")
	# 	elif command_result.status == "error":
	# 		is_successful = False
	# 		general_report.append(f"- ❌ ERROR: {summarizer(command_result)}")
	# 	elif command_result.status == "warn":
	# 		has_warnings = True
	# 		general_report.append(f"- ⚠️ WARN: {summarizer(command_result)}")

	cost_report = f"**Custo da Execução**: R${estimated_total_cost:.2f}"
	log(cost_report)

	# Sort and log the general report
	general_report = sorted(general_report, reverse=True)
	general_report = "**Resumo**:\n" + "\n".join(general_report)
	log(general_report)

	# # Get Parameters
	# param_report = ["**Parametros**:"]
	# for key, value in prefect.context.get("parameters").items():
	# 	if key == "rename_flow":
	# 		continue
	# 	if value:
	# 		param_report.append(f"- {key}: `{value}`")
	# param_report = "\n".join(param_report)
	# param_report += " \n"

	# fully_successful = is_successful and running_results.success
	# include_report = has_warnings or (not fully_successful)

	# # DBT - Sending Logs to Discord
	# command = prefect.context.get("parameters").get("command")
	# emoji = "❌" if not fully_successful else "✅"
	# complement = "com Erros" if not fully_successful else "sem Erros"
	# message = (
	# 		f"{param_report}\n{cost_report}\n{general_report}"
	# 		if include_report
	# 		else f"{param_report}\n{cost_report}"
	# )

	# send_discord_message(
	# 	title=f"{emoji} Execução `dbt {command}` finalizada {complement}",
	# 	message=message,
	# 	file_path=log_path,
	# 	monitor_slug="dbt-runs",
	# )

	# if not fully_successful:
	# 	return Failed(general_report)


@task
def get_dbt_target_from_environment(environment: str, requested_target: str = None):
	"""
	Retorna o `target` para o comando dbt baseado no ambiente executado
	"""
	requested_target = process_null_str(requested_target)

	# Se não foi requisitado nenhum target específico
	if not requested_target:
		# Confere se o `environment` é um dos valores permitidos
		is_valid = environment_is_valid(environment=environment, raise_if_not=False)
		# Retorna o próprio environment, caso seja válido
		return environment if is_valid else "dev"

	# Nem todo target existe/é permitido
	# https://github.com/prefeitura-rio/queries-rj-sms/blob/master/profiles.yml
	allowed_targets = [
		"prod",  # rj-sms     (dataset.table)
		"dev",  # rj-sms-dev (username__dataset.table)
		"ci",  # rj-sms-dev (dataset.table)
		"sandbox",  # rj-sms-sandbox (dataset.table)
	]  # fmt=skip

	if requested_target in allowed_targets:
		return requested_target

	raise ValueError(f"`target` requisitado, '{requested_target}', não é válido!")


@task
def download_dbt_artifacts_from_gcs(dbt_path: str, environment: str):
	"""
	Baixa os dbt artifacts do Google Cloud Storage
	"""
	gcs_bucket = dbt_constants.GCS_BUCKET.value[environment]
	target_base_path = os.path.join(dbt_path, "target_base")

	if os.path.exists(target_base_path):
		shutil.rmtree(target_base_path, ignore_errors=False)
		os.makedirs(target_base_path)

	try:
		download_from_bucket(target_base_path, gcs_bucket)
		log(f"dbt artifacts baixados do bucket: {gcs_bucket}")
		return target_base_path
	except Exception as e:
		log(f"Erro baixando dbt artifacts do bucket: {e}", level="error")
		return None


@task
def should_upload_artifacts(command: str):
	"""Confere se `command` é `"build"` ou `"source freshness"`"""
	return command in ["build", "source freshness"]


@task
def upload_dbt_artifacts_to_gcs(dbt_path: str, environment: str):
	"""
	Faz upload de dbt artifacts para o Google Cloud Storage
	"""
	dbt_artifacts_path = os.path.join(dbt_path, "target")
	gcs_bucket = dbt_constants.GCS_BUCKET.value[environment]
	upload_to_cloud_storage(dbt_artifacts_path, gcs_bucket)
	log(f"dbt artifacts enviados para o bucket: {gcs_bucket}")
