# -*- coding: utf-8 -*-

import os
import re
from typing import List

import pandas as pd

from dbt.contracts.results import RunResult, SourceFreshnessResult, NodeResult
from dbt.artifacts.resources import Time as DbtTime

from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log


def process_dbt_logs(log_path: str) -> pd.DataFrame:
  """
  Processamento do conteúdo de um arquivo de logs do dbt

  Args:
          log_path(str): Caminho para o arquivo de logs do dbt

  Returns:
          out(DataFrame):
                  DataFrame Pandas contendo as entradas do log, pós-parsing
  """
  with open(log_path, "r", encoding="utf-8", errors="ignore") as log_file:
    log_content = log_file.read()

  # Ex.:
  #   15:16:29.425453 [debug] [ThreadPool]: Opening a new connection, ...
  #                    \x1b = ESC
  result = re.split(r"(\x1b\[0m\d{2}:\d{2}:\d{2}\.\d{6})", log_content)
  parts = [part.strip() for part in result][1:]

  split_log = []
  for i in range(0, len(parts), 2):
    # ex.: "15:16:29.42"
    time = re.sub(r"\x1b\[0m", "", parts[i])[:-4]
    # ex.: "[debug]"
    level = parts[i + 1][1:6].replace(" ", "")
    # ex.: "[MainThread]: Up to date!"
    text = re.sub(r"\x1b\[[0-9]+m", "", parts[i + 1][7:]).strip()
    split_log.append((time, level, text))

  full_logs = pd.DataFrame(split_log, columns=["time", "level", "text"])
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
    text: str = row["text"]
    text = re.sub(
      # Remove '[Thread-# (]:' do início
      r"^\[Thread-[0-9]+ \(?\]:",
      "-",
      # Remove '[MainThread]:'/'[ThreadPool]:' do início
      text.removeprefix("[MainThread]:").removeprefix("[ThreadPool]:").strip(),
    )
    if len(text) <= 0:
      continue
    if text.lower().startswith(
      (
        "install",
        "updated version",
        "updates available",
        "up to date!",
        "unable to do partial",
      )
    ):
      continue
    # Pulamos warnings de deprecation porque no final vem um
    # "DeprecationsSummary" que lista a quantidade de cada
    if re.match(r"\[WARNING\]\[[A-Za-z]+Deprecation\]:", text):
      continue

    time = str(row["time"])
    level = str(row["level"]).ljust(5, " ")
    report.append(f"{time} [{level}]   {text}")
  report = "\n".join(report)
  log(f"Logs do DBT: {report}")

  dirpath = create_tmp_data_folder()
  filepath = os.path.join(dirpath, "dbt_log.txt")

  with open(filepath, "w+", encoding="utf-8") as log_file:
    log_file.write(report)

  return filepath


#####
# Sumarizador de resultados do DBT
#####


class Summarizer:
  """
  Produz resumos de execução a partir do resultado de um comando dbt.
  Possui suporte para `RunResult` e `SourceFreshnessResult`.
  """

  def __call__(self, result: NodeResult):
    status = getattr(result, "status", None)

    # Execução normal (ex.: dbt run, dbt build)
    if isinstance(result, RunResult):
      if status == "error":
        return f"`{result.node.name}`\n{result.message.replace('__', '_')}\n"

      relation_name = result.node.relation_name.replace("`", "").strip()
      if status in ("success", "warn", "fail"):
        return f"`{result.node.name}`\n{result.message}:```{relation_name}```\n"
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
