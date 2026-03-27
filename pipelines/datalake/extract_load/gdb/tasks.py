# -*- coding: utf-8 -*-
import os
import re
from time import sleep
import unicodedata

import pandas as pd

from pipelines.utils.cleanup import cleanup_columns_for_bigquery, jsonify_dataframe
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import now
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .utils import format_reference_date


@task
def run_conversion(filepath: str):
	# TODO
	log(f"Convertendo '{filepath}'....")
	log("(é mentira)")
	return "/tmp/data"


# TODO: transformar isso em função genérica
@task
def upload_csv_as_table(
	csv_path: str,
	dataset: str,
	uri: str,
	refdate: str = None,
	lines_per_chunk: int = 100_000,
):
	# Lemos o CSV em pedaços para não estourar a memória
	LINES_PER_CHUNK = int(lines_per_chunk or 100_000)

	filename = os.path.basename(csv_path)
	table_name = filename.removesuffix(".csv").strip()
	# Remove potenciais caracteres problemáticos em nomes de tabelas
	table_name = re.sub(
		r"_{3,}",
		"__",  # Limita underlines consecutivos a 2
		re.sub(r"[^a-z0-9_]", "_", unicodedata.normalize("NFKD", table_name).lower()),
	)

	csv_reader = pd.read_csv(
		csv_path, dtype="unicode", na_filter=False, chunksize=LINES_PER_CHUNK
	)
	log(f"Fazendo upload de '{table_name}'")

	# Iteramos por cada pedaço do CSV
	for j, chunk in enumerate(csv_reader):
		# Cria um dataframe a partir do chunk
		df = pd.DataFrame(chunk)
		log(f"Lendo pedaço #{j + 1} (até {LINES_PER_CHUNK} linhas)")

		# (acho que isso nem é mais possível, porque não entraria no `for`)
		if df.empty:
			log(f"{table_name} vazia; pulando")
			continue

		# Limpa nomes de colunas
		df = cleanup_columns_for_bigquery(df)
		# Transforma cada linha em um JSON
		df = jsonify_dataframe(df)

		# Metadados
		df["_source_file"] = uri
		df["_loaded_at"] = now()
		df["data_particao"] = format_reference_date(refdate, uri)

		log(
			f"Fazendo upload de tabela '{table_name}' via DataFrame: "
			f"{len(df)} linha(s); coluna(s) {list(df.columns)}"
		)
		attempt = 0
		MAX_UPLOAD_ATTEMPTS = 5
		while True:
			attempt += 1
			try:
				# Chama a task de upload
				upload_df_to_datalake_task.run(
					df=df,
					dataset_id=dataset,
					table_id=table_name,
					partition_column="data_particao",
					dump_mode="append",
				)
				break
			# Aqui deu erro de conexão comigo algumas vezes:
			# > socket.gaierror: [Errno -3] Temporary failure in name resolution
			# > httplib2.error.ServerNotFoundError:
			#     Unable to find the server at cloudresourcemanager.googleapis.com
			# > http.client.RemoteDisconnected: Remote end closed connection without response
			# > "Something went wrong while setting permissions for BigLake service account […]"
			# Então pescamos por um erro e tentamos mais uma vez por via das dúvidas
			except Exception as e:
				# Se já tentamos N vezes, desiste e dá erro
				if attempt > MAX_UPLOAD_ATTEMPTS:
					raise e
				# Senão, pausa por uns segundos e tenta de novo
				log(f"{repr(e)}; esperando 5s e  and retrying", level="warning")
				sleep(5)
	# Fim!
