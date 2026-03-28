# -*- coding: utf-8 -*-
# /// script
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "dbfread==2.0.7",
#   "pyreaddbc==1.1.0",
#   "numpy>=1.21,<2",
#   "setuptools==80.10.2"
# ]
# ///
# "Por que Python 3.11?"
# > `pyreaddbc==1.1.0 depends on pandas>=1.4.3,<2
#   and [pipelines] depends on pandas>=3.0.1`
# > "pandas>=1.4.3,<2" => v1.5.3, que só suporta
#   até Python 3.11
# [Ref] https://pandas.pydata.org/pandas-docs/version/1.5/getting_started/install.html
"""
Executado com `uv run --script dbc2csv.py (filename)`
"""

import os
import sys
import csv
from datetime import datetime
from zoneinfo import ZoneInfo

import pyreaddbc
from dbfread import DBF


def standardize_filename(filepath: str):
	filepath = str(filepath).strip("\"' ")
	# Confere se arquivo realmente existe
	if not os.path.exists(filepath):
		raise RuntimeError(f"Arquivo '{filepath}' não existe!")

	# Separa caminho do file extension
	(raw_filename, suffix) = filepath.rsplit(".", maxsplit=1)

	# Se o arquivo é DBC ou DBF, com maiúsculas,
	# renomeia para minúsculas
	if suffix == "DBC":
		new_filename = f"{raw_filename}.dbc"
		os.rename(filepath, new_filename)
		return (new_filename, "dbc")
	if suffix == "DBF":
		new_filename = f"{raw_filename}.dbf"
		os.rename(filepath, new_filename)
		return (new_filename, "dbf")

	# Se o arquivo já era DBC ou DBF com minúsculas,
	# não faz nada
	if suffix in ("dbc", "dbf"):
		return (filepath, suffix)

	# Se chegamos aqui, o arquivo não é DBC nem DBF; erro
	raise ValueError(f"Esperado arquivo .dbc ou .dbf, recebeu '.{suffix}': '{filepath}'")


def dbc2csv(filepath: str):
	(filepath, suffix) = standardize_filename(filepath)
	raw_filepath = filepath.removesuffix(f".{suffix}")

	# Se recebemos um DBC, precisamos descomprimí-lo
	# para um DBF; mas esse DBF é temporário, e vamos
	# apagá-lo depois
	should_delete_dbf = False
	if suffix == "dbc":
		print("Descomprimindo DBC para DBF...")
		pyreaddbc.dbc2dbf(filepath, f"{raw_filepath}.dbf")
		should_delete_dbf = True

	# Aqui sabemos que temos um DBF; convertemos para CSV
	print("Convertendo DBF para CSV...")
	# TODO: permitir customização de codificação?
	table = DBF(f"{raw_filepath}.dbf", encoding="iso-8859-1")
	with open(f"{raw_filepath}.csv", "w", encoding="utf-8") as file:
		writer = csv.writer(file)
		header = table.field_names
		print(f"Cabeçalho: {str(header)}")
		writer.writerow(header)

		for i, record in enumerate(table):
			if i % 50_000 == 0:
				now = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime("%H:%M:%S")
				print(f"{now} - Linha {i}")
			writer.writerow(list(record.values()))

	# Limpa arquivo DBF se for preciso
	if should_delete_dbf:
		os.remove(f"{raw_filepath}.dbf")

	# Fim :)
	print("Arquivo convertido!")


if __name__ == "__main__":
	if len(sys.argv) == 2:
		print(f"Extraindo '{sys.argv[1]}'...")
		dbc2csv(sys.argv[1])
	else:
		print("$ uv run --script dbc2csv.py (filename)")
