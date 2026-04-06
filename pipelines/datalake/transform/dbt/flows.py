# -*- coding: utf-8 -*-

from pipelines.utils.prefect import flow, flow_config, rename_flow_run
from pipelines.utils.git import download_gh_repo
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.constants import constants as global_consts

from .schedules import schedules
from .tasks import (
	create_dbt_report,
	download_dbt_artifacts_from_gcs,
	estimate_dbt_costs,
	execute_dbt,
	get_dbt_target_from_environment,
	should_upload_artifacts,
	upload_dbt_artifacts_to_gcs,
)


@flow(
	name="DataLake - Transformação - DBT",
	state_handlers=[handle_flow_state_change],
	owners=[global_consts.CIT_ID.value],
)
def sms_execute_dbt(
	command: str = "test",
	select: str | None = None,
	exclude: str | None = None,
	flag: str | None = None,
	target: str | None = None,
	rename_flow: bool = False,
	send_discord_report: bool = False,
	environment: str = "dev",
):
	#######################################
	##  1) Pré-configurações, dependências
	#######################################
	if rename_flow:
		rename_flow_run(
			new_name=(
				f"dbt {command}"
				+ (f" --select {select}" if select else "")
				+ (f" --exclude {exclude}" if exclude else "")
				+ (f" --target {target}" if target else "")
			)
		)

	# Baixa o código atual do repositório
	repo_path = download_gh_repo(
		repo="prefeitura-rio/queries-rj-sms",
		branch="master",
		if_destination_exists="delete",
	)

	fixed_target = get_dbt_target_from_environment(
		environment=environment, requested_target=target
	)

	# Instala dependências do dbt
	install_dbt_packages = execute_dbt(
		repository_path=repo_path, target=fixed_target, command="deps"
	)

	# Baixa
	target_base_path = download_dbt_artifacts_from_gcs(
		dbt_path=repo_path, environment=environment, wait_for=[install_dbt_packages]
	)

	#######################################
	##  2) Executa o comando DBT
	#######################################
	execution_info = execute_dbt(
		repository_path=repo_path,
		state=target_base_path,
		target=fixed_target,
		command=command,
		select=select,
		exclude=exclude,
		flag=flag,
	)

	estimated_total_cost = estimate_dbt_costs(
		execution_info=execution_info, environment=environment
	)

	if send_discord_report:
		create_dbt_report(
			execution_info=execution_info,
			estimated_total_cost=estimated_total_cost,
			repository_path=repo_path,
		)

	#######################################
	##  3) Faz upload para GCS
	#######################################
	should_upload = should_upload_artifacts(command=command)
	if should_upload:
		upload_dbt_artifacts_to_gcs(
			dbt_path=repo_path, environment=environment, wait_for=[execution_info]
		)


_flows = [flow_config(flow=sms_execute_dbt, schedules=schedules)]
