# -*- coding: utf-8 -*-
import re
import pandas as pd


def guarantee_proper_column_name(column: str) -> str:
	"""
	Remove todos os caracteres não-alfanuméricos (+ `_`).
	Caso a coluna só possua dígitos após a remoção, adiciona um `_` no início.
	"""
	column_normalized = re.sub(r"[^a-zA-Z0-9_]+", "", column)
	if len(column_normalized) <= 0:
		raise ValueError(f"Coluna '{column}' só possui caracteres inválidos!")
	try:
		int(column_normalized)
		return f"_{column_normalized}"
	except ValueError as _:
		return column_normalized


def remove_column_accents(dataframe: pd.DataFrame) -> list:
	"""
	Remove acentos e outras marcas (p.ex. cedilha) de colunas de um DataFrame
	"""
	columns = [str(column) for column in dataframe.columns]
	dataframe.columns = columns
	return list(
		dataframe.columns.str.normalize("NFKD")
		.str.encode("ascii", errors="ignore")
		.str.decode("utf-8")
		.map(lambda x: x.strip())
		.str.replace(" ", "_")
		.str.replace("/", "_")
		.str.replace("-", "_")
		.str.replace("\a", "_")
		.str.replace("\b", "_")
		.str.replace("\n", "_")
		.str.replace("\t", "_")
		.str.replace("\v", "_")
		.str.replace("\f", "_")
		.str.replace("\r", "_")
		.str.lower()
		.map(guarantee_proper_column_name)
	)


def process_null_str(val: str | None) -> str | None:
	"""
	Retorna `None` caso o valor seja `None` ou `""`;
	caso contrário, retorna `valor.strip()`.
	"""
	if not val:
		return None
	val = str(val).strip()
	if not val:
		return None
	return val
