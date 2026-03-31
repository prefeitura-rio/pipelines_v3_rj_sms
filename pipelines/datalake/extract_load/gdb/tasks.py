# -*- coding: utf-8 -*-
import os
from time import sleep
from typing import Literal

import pandas as pd

from pipelines.utils.cleanup import (
	cleanup_bigquery_name,
	cleanup_columns_for_bigquery,
	jsonify_dataframe,
)
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import now
from pipelines.utils.io import create_tmp_data_folder, get_file_size
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.process import run_command

from .utils import format_reference_date


@task
def run_conversion(filepath: str):
	# Passo a passo:
	# - Carregar variáveis de ambiente de 32-bit
	# - `gbak` pra converter GDB pra FBK
	# - Carregar variáveis de ambiente de 64-bit
	# - `gbak` pra converter FBK pra FDB
	# - Executa gdb2csv.py

	# Pegamos o PATH antes de adicionar o Firebird 32-bit;
	# vamos usar ele em breve quando trocarmos pra 64-bit
	no_firebird_path = os.environ["PATH"]

	def set_env_vars(original_path: str, bitness: Literal["32bit", "64bit"]):
		os.environ["PATH"] = (
			f"/opt/{bitness}/firebird"
			f":/opt/{bitness}/firebird/bin"
			f":/opt/{bitness}/firebird/lib"
			f":{original_path}"
		)
		os.environ["FIREBIRD"] = f"/opt/{bitness}/firebird"
		os.environ["LD_LIBRARY_PATH"] = f"/opt/{bitness}/firebird/lib"

	# Carrega variáveis 32-bit
	set_env_vars(no_firebird_path, "32bit")
	log("Criando backup (GDB→FBK, 32-bit)...")
	directory = os.path.dirname(filepath)
	fbk_filepath = os.path.join(directory, "backup.fbk")
	run_command(
		[
			# Usamos o caminho absoluto por via das dúvidas
			"/opt/32bit/firebird/bin/gbak",
			"-backup_database",
			"-verify",
			"-transportable",  # Agnóstico a versões/plataformas
			filepath,
			fbk_filepath,
			"-user",
			"SYSDBA",
			"-password",
			"masterkey",
		],
		print_stdout=False,
	)
	# Garante que o arquivo de saída existe, possui conteúdo
	fbk_size = get_file_size(
		fbk_filepath, pretty=True, raise_if_missing=True, raise_if_not_file=True
	)
	log(f"Arquivo '{fbk_filepath}' possui tamanho {fbk_size}")
	# Apaga GDB agora que já foi exportado
	os.remove(filepath)

	# Carrega variáveis 64-bit
	set_env_vars(no_firebird_path, "64bit")
	log("Carregando backup (FBK→FDB, 64-bit)...")
	fdb_filepath = os.path.join(directory, "backup.fdb")
	run_command(
		[
			"/opt/64bit/firebird/bin/gbak",
			"-create_database",
			"-verify",
			fbk_filepath,
			fdb_filepath,
			"-user",
			"SYSDBA",
			"-password",
			"masterkey",
		],
		print_stdout=False,
	)
	# Garante que o arquivo de saída existe, possui conteúdo
	fdb_size = get_file_size(
		fdb_filepath, pretty=True, raise_if_missing=True, raise_if_not_file=True
	)
	log(f"Arquivo '{fdb_filepath}' possui tamanho {fdb_size}")
	# Apaga FBK agora que já foi carregado
	os.remove(fbk_filepath)

	output_folder = create_tmp_data_folder()
	log("Executando script de conversão FDB→CSV")
	run_command(
		[
			"uv",
			"run",
			"--script",
			"pipelines/datalake/extract_load/gdb/scripts/fdb2csv.py",
			fdb_filepath,
			output_folder,
		]
	)

	output_files = os.listdir(output_folder)
	log(
		f"Pasta de destino '{output_folder}' possui "
		f"{len(output_files)} arquivo(s): "
		f"{output_files[:10]} (primeiros 10)"
	)

	return output_folder


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
	table_name = cleanup_bigquery_name(table_name)

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
