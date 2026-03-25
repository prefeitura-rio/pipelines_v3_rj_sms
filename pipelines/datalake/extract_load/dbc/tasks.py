# -*- coding: utf-8 -*-
import os

from prefect.variables import Variable

from pipelines.utils.cleanup import prettify_byte_size
from pipelines.utils.google import dissect_gcs_uri, upload_to_cloud_storage
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
	stdout = run_command(command)
	if stdout:
		log(f"Output:\n{stdout}")
	# Se não deu erro, então aqui devemos ter um arquivo CSV
	# com mesmo nome do DBC/DBF original
	(filepath, _) = filepath.rsplit(".", maxsplit=1)
	output_filepath = f"{filepath}.csv"
	if not os.path.exists(output_filepath):
		raise RuntimeError(f"Arquivo '{output_filepath}' não existe!")

	filesize = os.path.getsize(output_filepath)
	log(f"Arquivo '{output_filepath}' tem tamanho {prettify_byte_size(filesize)}")
	return output_filepath


@task
def upload_result(original_uri: str, filepath: str):
	uri = dissect_gcs_uri(original_uri)
	bucket = uri["bucket"]
	blob = uri["blob"]
	log(f"Fazendo upload de '{filepath}' para 'gs://{bucket}/{blob}'")
	# Faz upload do arquivo no mesmo bucket e caminho
	# do arquivo original
	upload_to_cloud_storage(filepath, bucket, blob)

	(_, filename) = filepath.rsplit("/", maxsplit=1)
	Variable.set(
		original_uri,
		(
			f"gs://{bucket}/{blob}/{filename}"
			if len(blob) > 0
			else f"gs://{bucket}/{filename}"
		),
		tags=["gcs", "conversão", "dbc2csv"],
		overwrite=True
	)
