# -*- coding: utf-8 -*-
# /// script
# requires-python = ">=3.13"
# dependencies = [
#   "fdb==2.0.4",
# ]
# ///
"""
Executado com `uv run --script fdb2csv.py (input-file) (output-dir)`
"""

import csv
import os
import sys
import fdb


def fdb2csv(input_file: str, output_dir: str):
	input_file = str(input_file).strip("\"' ")
	# Garante que arquivo de entrada existe
	if not os.path.exists(input_file):
		raise RuntimeError(f"Arquivo '{input_file}' não existe!")

	if not input_file.lower().endswith(".fdb"):
		print(
			"Esse script espera um arquivo FDB! Provável erro à frente", file=sys.stderr
		)

	# Garante existência da pasta de saída
	output_dir = str(output_dir).strip("\"' ")
	os.makedirs(output_dir, exist_ok=True)

	# Cria conexão com o arquivo FDB
	with fdb.connect(
		database=input_file, user="SYSDBA", password="masterkey", charset="ISO8859_1"
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
			print(f"Encontrada(s) {len(tables)} tabela(s)")

			for table in tables:
				csv_filepath = os.path.join(output_dir, f"{table}.csv")
				cur.execute(f'SELECT * FROM "{table}"')
				headers = [col_desc[0] for col_desc in cur.description]
				print(f"Tabela '{table}' tem {len(headers)} coluna(s): {headers}")

				with open(csv_filepath, "w", newline="", encoding="utf-8") as csv_file:
					writer = csv.writer(csv_file, quoting=csv.QUOTE_MINIMAL)
					writer.writerow(headers)
					# Itera pelo cursor da conexão, o que diminui uso de memória
					row_count = 0
					for row in cur:
						writer.writerow(row)
						row_count += 1

				print(f"\tExportada(s) {row_count} linha(s)!")
		finally:
			cur.close()


if __name__ == "__main__":
	if len(sys.argv) == 3:
		print(f"Extraindo '{sys.argv[1]}' para '{sys.argv[2]}'...")
		fdb2csv(sys.argv[1], sys.argv[2])
	else:
		print("$ uv run --script dbc2csv.py (input-file) (output-dir)")
