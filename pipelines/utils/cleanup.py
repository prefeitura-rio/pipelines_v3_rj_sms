# -*- coding: utf-8 -*-
import re
from typing import List, Optional
import unicodedata
import pandas as pd


def cleanup_bigquery_name(name: str) -> str:
	"""
	Limpa nome de colunas ou tabelas no BigQuery
	- Remove acentos de letras
	- Substitui tudo que não seja letra ou dígito por `_`
	"""
	stripped = str(name).strip()
	if not name or len(stripped) <= 0:
		raise ValueError("Nome não pode ser nulo ou vazio!")
	# Normaliza: Á -> A´ -> A
	normalized = (
		# Separa acentos das letras
		unicodedata.normalize("NFKD", stripped)
		# Apaga tudo que não seja ASCII (p.ex. acentos, cedilha, ...)
		.encode("ascii", errors="ignore")
		.decode("utf-8")
	)
	# Troca símbolos, espaços, etc por '_'
	only_alphanumeric = re.sub(r"[^A-Za-z0-9_]", "_", normalized)
	# Remove underlines excessivos i.e. 'a_______b___' -> 'a__b'
	no_excessive_underlines = re.sub(r"_{3,}", "__", only_alphanumeric).strip("_")
	if len(no_excessive_underlines) <= 0:
		raise ValueError(f"Nome '{name}' fica vazio após limpeza!")
	# Se só sobraram dígitos, precisamos prefixar com '_'
	if re.fullmatch(r"[0-9]+", no_excessive_underlines):
		return f"_{no_excessive_underlines}"
	# Senão, já está pronto
	return no_excessive_underlines


def cleanup_columns_for_bigquery(
	df: pd.DataFrame, lowercase: bool = False, raise_on_repeats: bool = False
):
	"""
	Remove acentos e outras marcas (p.ex. cedilha) de colunas de um DataFrame,
	preparando-o para upload para o BigQuery. A substituição é feita in-place;
	isto é, as colunas do DataFrame passado serão modificadas pela função.

	Caso seja desejado, nomes de colunas podem ser convertidos para lowercase
	via `lowercase=True`. Por padrão, não há conversão nenhuma.

	Em caso de colunas com nomes repetidos pós tratamento, por padrão,
	a função irá adicionar "_1" à duplicata (incrementando em caso de mais
	repetições). Para, ao invés disso, disparar um erro, passe
	`raise_on_repeats=True`.
	"""
	column_mapping = dict()
	existing_columns = set()
	for col in df.columns:
		# Limpa nome da coluna
		clean_col = cleanup_bigquery_name(col)
		if lowercase:
			clean_col = clean_col.lower()
		# Se o nome da coluna já estiver em uso, adiciona
		# sufixo '_1', '_2', ... progressivamente até não
		# haver mais repetições
		clean_col_no_repeats = clean_col  # Nome começa sem sufixo
		repeat_count = 0
		while clean_col_no_repeats in existing_columns:  # Se aparece no Set
			# Se o usuário não quer sufixos nas colunas, morre aqui
			if raise_on_repeats:
				raise ValueError(
					f"Coluna '{col}' se torna '{clean_col}' após tratamento, "
					"nome já em uso no DataFrame!"
				)
			# Caso contrário, soma 1 ao contador e retenta com novo sufixo
			repeat_count += 1
			clean_col_no_repeats = f"{clean_col}_{repeat_count}"
		# Salva nome novo da coluna garantidamente não repetido
		existing_columns.add(clean_col_no_repeats)
		# Cria mapeamento do nome original -> tratado
		column_mapping[col] = clean_col_no_repeats
	# Após processar todas as colunas, substitui nomes no DataFrame
	df.rename(columns=column_mapping, inplace=True)
	return df


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
