# -*- coding: utf-8 -*-
import csv
import os
import re

from pipelines.utils.datetime import now
from pipelines.utils.google import dissect_gcs_uri
from pipelines.utils.logger import log


def format_reference_date(refdate: str | None, gcs_uri: str) -> str:
	# Se não há data especificada
	if refdate is None or not refdate:
		# Tentamos pegar do nome do arquivo
		# Os arquivos normalmente terminam ou com MMYYYY ou YYYYMM
		uri = dissect_gcs_uri(gcs_uri)
		filename = uri["filename_no_ext"]

		# Se o arquivo termina com 6 dígitos
		if re.search(r"[0-9]{6}$", filename):
			# Pegamos somente os dígitos
			dt = filename[-6:]
			# Se os dois primeiros são > 12, então a data começa com ano
			if int(dt[:2]) > 12:
				# [YYYY]MM
				year = dt[:4]
				# YYYY[MM]
				month = dt[4:]
			# Senão, começa com <= 12, e não estamos em um ano <1200
			# Então o mês vem primeiro
			else:
				# [MM]YYYY
				month = dt[:2]
				# MM[YYYY]
				year = dt[2:]

			# Filtros finais de data realista
			if 2000 < int(year) < 2100 and 0 < int(month) < 13:
				return f"{year}-{month}-01"
		# Aqui, o arquivo não termina com mês e ano; não temos
		# ideia de qual data ele se refere, e não recebemos
		# `refdate` como parâmetro. Então a melhor opção é a
		# data atual :/
		dt = now()
		return dt.strftime("%Y-%m-01")

	# Aqui, recebemos data de referência! Uhules
	refdate = str(refdate).strip()
	# YYYY-MM?-DD?
	if re.search(r"^[0-9]{4}$", refdate):
		return f"{refdate}-01-01"
	if re.search(r"^[0-9]{4}[\-\/][0-9]{2}$", refdate):
		return f"{refdate}-01".replace("/", "-")
	if re.search(r"^[0-9]{4}[\-\/][0-9]{2}[\-\/][0-9]{2}$", refdate):
		return f"{refdate}".replace("/", "-")
	raise ValueError(
		f"Formato de data referência é 'YYYY(-MM(-DD)?)?'; "
		f"valor recebido, '{refdate}', não encaixa"
	)


######
# Funções específicas para interação com o Firebird


def detect_charset(input_file: str):
	import fdb  # type: ignore

	# Se executando localmente, você vai esbarrar em um erro de falta
	# de biblioteca aqui; você precisa configurar LD_LIBRARY_PATH=/opt/64bit/firebird/lib
	# no seu ambiente antes de executar o código
	with fdb.connect(
		database=input_file, user="SYSDBA", password="masterkey", charset="NONE"
	) as conn:
		cur = conn.cursor()
		cur.execute("SELECT RDB$CHARACTER_SET_NAME FROM RDB$DATABASE;")
		result = cur.fetchone()
		if result and result[0]:
			charset = result[0].strip() or "NONE"
			log(f"Charset é '{charset}'")
			return charset
	log("Charset não definido; retornando 'NONE'")
	return "NONE"


def fdb2csv(input_file: str, output_dir: str):
	"""
	Converte um arquivo .FDB em arquivos .CSV, um para cada tabela do banco de dados
	original. Recebe o caminho do arquivo FDB e o caminho da pasta onde depositar os
	CSVs.
	"""
	# Se executando localmente, você precisa instalar fdb (`uv add 'fdb==2.0.4'`)
	# [e instalar dois Firebird CS v2.5, 32-bit em /opt/32bit/firebird e
	# 64-bit em /opt/64bit/firebird, como no Dockerfile], mas como só esse flow
	# usa fdb, não faça commit do seu pyproject.toml/uv.lock
	import fdb  # type: ignore

	input_file = str(input_file).strip("\"' ")
	# Garante que arquivo de entrada existe
	if not os.path.exists(input_file):
		raise RuntimeError(f"Arquivo '{input_file}' não existe!")

	if not input_file.lower().endswith(".fdb"):
		log("Esse script espera um arquivo FDB! Provável erro à frente", level="error")

	# Garante existência da pasta de saída
	output_dir = str(output_dir).strip("\"' ")
	os.makedirs(output_dir, exist_ok=True)
	charset = detect_charset(input_file)

	# Cria conexão com o arquivo FDB
	with fdb.connect(
		database=input_file, user="SYSDBA", password="masterkey", charset=charset
	) as conn:
		try:
			cur = conn.cursor()
			# Lista todas as tabelas na database
			# [Ref] https://ib-aid.com/download/docs/firebird-language-reference-2.5/fblangref-appx04-relations.html
			cur.execute("""
			SELECT RDB$RELATION_NAME
			FROM RDB$RELATIONS
			WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG = 0)
				AND RDB$VIEW_BLR IS NULL
			ORDER BY RDB$RELATION_NAME
			""")

			tables = [row[0].strip() for row in cur.fetchall()]
			log(f"Encontrada(s) {len(tables)} tabela(s)")

			for table in tables:
				csv_filepath = os.path.join(output_dir, f"{table}.csv")
				cur.execute(f'SELECT * FROM "{table}"')
				headers = [col_desc[0] for col_desc in cur.description]
				log(f"Tabela '{table}' tem {len(headers)} coluna(s): {headers}")

				with open(csv_filepath, "w", newline="", encoding="utf-8") as csv_file:
					writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
					writer.writerow(headers)
					# Itera pelo cursor da conexão, o que diminui uso de memória
					row_count = 0
					for row in cur:
						writer.writerow(row)
						row_count += 1

				log(f"\tExportada(s) {row_count} linha(s)!")
		finally:
			cur.close()
