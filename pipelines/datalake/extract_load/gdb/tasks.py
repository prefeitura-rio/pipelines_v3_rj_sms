# -*- coding: utf-8 -*-

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def run_conversion(filepath: str):
	# TODO
	log(f"Convertendo '{filepath}'....")
	log("(é mentira)")
	return "/tmp/data"
