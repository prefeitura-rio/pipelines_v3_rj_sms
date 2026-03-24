# -*- coding: utf-8 -*-

from datetime import datetime
import os

from prefect.states import Failed
from dbt.cli.main import dbtRunner, dbtRunnerResult

from pipelines.constants import constants
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


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

	Returns:
		dict: FIXME
	"""
	commands = command.split(" ")

	cli_args = commands + ["--profiles-dir", repository_path, "--project-dir", repository_path]

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
	start_time = datetime.now(tz=constants.TIMEZONE.value)
	running_result: dbtRunnerResult = dbt_runner.invoke(cli_args)
	end_time = datetime.now(tz=constants.TIMEZONE.value)
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

	# FIXME: ler o arquivo de logs?

	return {
		"command": " ".join(cli_args),
		"running_result": running_result,
		"execution_time": execution_time,
		"start_time": start_time,
		"end_time": end_time,
		"log_path": log_path,
	}
