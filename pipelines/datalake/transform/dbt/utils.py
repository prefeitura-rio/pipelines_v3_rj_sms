# -*- coding: utf-8 -*-

import os
import re
from typing import List

import pandas as pd

from dbt.contracts.results import RunResult, SourceFreshnessResult, NodeResult
from dbt.artifacts.resources import Time as DbtTime

from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log


def process_dbt_logs(log_path: str = "dbt_repository/logs/dbt.log") -> pd.DataFrame:
	"""
	Process the contents of a dbt log file and return a DataFrame containing the parsed log entries

	Args:
		log_path (str): The path to the dbt log file. Defaults to "dbt_repository/logs/dbt.log".

	Returns:
		pd.DataFrame: A DataFrame containing the parsed log entries.
	"""

	with open(log_path, "r", encoding="utf-8", errors="ignore") as log_file:
		log_content = log_file.read()

	result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
	parts = [part.strip() for part in result][1:]

	splitted_log = []
	for i in range(0, len(parts), 2):
		time = parts[i].replace(r"\x1b[0m", "")
		level = parts[i + 1][1:6].replace(" ", "")
		text = parts[i + 1][7:]
		splitted_log.append((time, level, text))

	full_logs = pd.DataFrame(splitted_log, columns=["time", "level", "text"])

	return full_logs


def log_to_file(logs: pd.DataFrame, levels: List[str] = None) -> str:
	"""
	Escreve logs em um arquivo e retorna o caminho do arquivo.

	Args:
		logs (pd.DataFrame):
			DataFrame com logs a serem escritos no arquivo.
		levels (list[str]):
			Quais níveis de logs devem ser escritos no arquivo.
			Por padrão, `["info", "error", "warn"]`.

	Returns:
		str: Caminho do arquivo criado
	"""
	if levels is None:
		levels = ["info", "error", "warn"]
	logs = logs[logs.level.isin(levels)]

	report = []
	for _, row in logs.iterrows():
		report.append(f"{row['time']} [{row['level'].rjust(5, ' ')}] {row['text']}")
	report = "\n".join(report)
	log(f"Logs do DBT: {report}")

	dirpath = create_tmp_data_folder()
	filepath = os.path.join(dirpath, "dbt_log.txt")

	with open(filepath, "w+", encoding="utf-8") as log_file:
		log_file.write(report)

	return filepath


#####
# Sumarizadores de resultados do DBT
#####


class Summarizer:
	"""
	Produz resumos de execução a partir do resultado de um comando dbt.
	Possui suporte para `RunResult` e `SourceFreshnessResult`.
	"""

	def __call__(self, result: NodeResult):
		status = getattr(result, "status", default=None)

		# Execução normal (ex.: dbt run, dbt build)
		if isinstance(result, RunResult):
			if status == "error":
				return f"`{result.node.name}`\n{result.message.replace('__', '_')}\n"

			relation_name = result.node.relation_name.replace("`", "")
			if status in ("warn", "fail"):
				return (
					f"`{result.node.name}`\n"
					f"{result.message}:"
					f"```select * from {relation_name}```\n"
				)
			raise ValueError(f"Status de resultado desconhecido: '{status}'")

		# Source freshness
		if isinstance(result, SourceFreshnessResult):
			relation_name = result.node.relation_name.replace("`", "")

			if status == "fail":
				return relation_name

			if status in ("warn", "error"):
				freshness = result.node.freshness
				# Aqui acessamos warn_after ou error_after a depender do status
				issue_after: DbtTime = getattr(freshness, f"{status}_after")
				criteria = f">={issue_after.count} {issue_after.period}"
				return f"`{relation_name}` ({criteria})"
			raise ValueError(f"Status de resultado desconhecido: '{status}'")

		# Outro resultado inesperado
		raise ValueError(f"Tipo de resultado dbt desconhecido: '{type(result)}'")
