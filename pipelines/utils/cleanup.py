# -*- coding: utf-8 -*-
import re
from typing import List, Optional
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


def jsonify_dataframe(
	df: pd.DataFrame, keep_columns: Optional[List[str] | str | None] = None
) -> pd.DataFrame:
	"""
	Converte todas as linhas de um dataframe em strings JSON.
	Isso pode afetar o objeto referenciado por `df`; faça uma cópia antes de usar
	essa função se você precisa acessar algum valor original posteriormente.

	Args:
		df (pd.DataFrame):
			Dataframe original.
		keep_columns (List[str] | str?):
			Lista opcional de nomes de colunas para não incluir no JSON.
			Se não especificado, o dataframe resultante terá uma única
			coluna, `json`. Se a string `"s"` é passada ao invés de uma lista,
			isso será equivalente a `keep_columns=["s"]`. Se todas as
			colunas do dataframe original estiverem na lista passada para
			`keep_columns`, uma coluna `"json"` de strings vazias será
			adicionada ao dataframe.

	Returns:
		df (DataFrame):
			Modified dataframe.

	Exemplo de uso:
	>>> a = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6], 'col3': [7, 8, 9]})
	>>> jsonify_dataframe(a)  # equivalente a keep_columns=[]
	.                          json
	0  {"col1":1,"col2":4,"col3":7}
	1  {"col1":2,"col2":5,"col3":8}
	2  {"col1":3,"col2":6,"col3":9}
	>>> jsonify_dataframe(a, keep_columns="col1")
	.                 json  col1
	0  {"col2":4,"col3":7}     1
	1  {"col2":5,"col3":8}     2
	2  {"col2":6,"col3":9}     3
	>>> jsonify_dataframe(a, keep_columns=["col1", "col3"])
	.         json  col1  col3
	0  {"col2":4}     1     7
	1  {"col2":5}     2     8
	2  {"col2":6}     3     9
	>>> jsonify_dataframe(a, keep_columns=["col1", "col2", "col3"])
	. json  col1  col2  col3
	0   {}     1     4     7
	1   {}     2     5     8
	2   {}     3     6     9
	"""
	keep_columns = keep_columns or []
	if isinstance(keep_columns, str):
		keep_columns = [keep_columns]

	# Colunas que queremos inserir no JSON
	json_columns = set(df.columns) - set(keep_columns)
	if len(json_columns) <= 0:
		df.insert(0, "json", "{}")
		return df

	# Aqui é dupla negativa: estamos REMOVENDO colunas que NÃO vão pro JSON
	# i.e. mantemos somente colunas que VÃO pro JSON
	df_json = df.drop(columns=list(keep_columns))
	# Oposto aqui: removemos colunas do JSON, i.e. mantemos as fora do JSON
	df_keep = df.drop(columns=list(json_columns))
	# Transforma colunas em JSON
	df_json["json"] = df_json.to_json(orient="records", lines=True).splitlines()
	# Remove colunas que foram pro JSON (então só resta a coluna do JSON)
	df_json.drop(columns=list(json_columns), inplace=True)
	# Merge do novo dataframe de JSON com as colunas anteriores
	# Em teoria teremos o mesmo número de linhas, na mesma ordem, ...
	_tmp_df = pd.merge(df_json, df_keep, left_index=True, right_index=True)
	return _tmp_df


def prettify_byte_size(byte_size: int, precision: int = 1):
	"""
	Converte um valor em bytes para uma unidade mais compacta

	>>> prettify_byte_size(10)
	'10 B'
	>>> prettify_byte_size(500_000)
	'488.3 KiB'
	>>> prettify_byte_size(5_000_000)
	'4.8 MiB'
	>>> prettify_byte_size(5_000_000, precision=0)
	'5 MiB'
	>>> prettify_byte_size(5_000_000, precision=2)
	'4.77 MiB'
	>>> prettify_byte_size(5_000_000, precision=4)
	'4.7684 MiB'
	>>> prettify_byte_size(500_000_000_000)
	'465.7 GiB'
	"""
	# [Ref] https://stackoverflow.com/a/58467404/4824627
	byte_size = int(byte_size)
	if byte_size <= 0:
		return "0 B"

	units = ("KiB", "MiB", "GiB", "TiB")
	# Cria lista com o valor em todas as unidades
	size_list = [f"{byte_size:,} B"]  # B separado porque não é :.1f
	size_list.extend(
		[
			f"{byte_size / (1024.0 ** (i + 1)):,.{precision}f} {unit}"
			for i, unit in enumerate(units)
		]
	)
	# Pega o último valor que não começa com "0." ou,
	# no caso de precision=0, que não começa com "0 "
	return [
		size for size in size_list if not (size.startswith("0.") or size.startswith("0 "))
	][-1]
