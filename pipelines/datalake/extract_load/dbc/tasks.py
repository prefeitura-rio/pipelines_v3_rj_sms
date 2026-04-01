# -*- coding: utf-8 -*-
import os

from pipelines.utils.cleanup import prettify_byte_size
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.process import run_command


@task
def run_conversion(filepath: str):
	command = [
		"uv",
		"run",
		"--script",
		"pipelines/datalake/extract_load/dbc/scripts/dbc2csv.py",
		f'"{filepath}"',
	]
	log(f"Executando comando '{' '.join(command)}' via subprocesso")
	run_command(command)  # Se deu erro, dispara aqui mesmo
	# Se não deu erro, então aqui devemos ter um arquivo CSV
	# com mesmo nome do DBC/DBF original
	(filepath, _) = filepath.rsplit(".", maxsplit=1)
	output_filepath = f"{filepath}.csv"
	if not os.path.exists(output_filepath):
		raise RuntimeError(f"Arquivo '{output_filepath}' não existe!")

	filesize = os.path.getsize(output_filepath)
	log(f"Arquivo '{output_filepath}' tem tamanho {prettify_byte_size(filesize)}")
	return output_filepath
