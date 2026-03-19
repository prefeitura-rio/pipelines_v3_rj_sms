# -*- coding: utf-8 -*-

from pipelines.utils.flow import flow
from pipelines.utils.git import download_gh_repo
from pipelines.utils.logger import log
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.constants import constants

# from .tasks import (
# 	...
# )
from .schedules import schedules


@flow(
	name="DataLake - Transformação - DBT",
	state_handlers=[handle_flow_state_change],
	owners=[
		constants.CIT_ID.value,
	],
)
def sms_execute_dbt(
	# command: str = "test",
	# select: str = None,
	# exclude: str = None,
	# flag: str = None,
	# target: str = None,
	# rename_flow: bool = False,
	# send_discord_report: bool = False,
	environment: str = "dev"
):
	path = download_gh_repo(
		repo="prefeitura-rio/queries-rj-sms",
		branch="master",
		if_destination_exists="delete"
	)
	log(path)
	# ...


_flows = [sms_execute_dbt]
_schedules = schedules
