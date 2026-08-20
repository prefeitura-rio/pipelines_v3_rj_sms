# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from pipelines.datalake.extract_load.siscan_web_laudos.scraper import run_scraper
from pipelines.utils.datetime import now_str, today
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

DATE_FORMAT = "%d/%m/%Y"


def _format_date(value: date | datetime) -> str:
  if isinstance(value, datetime):
    value = value.date()
  return value.strftime(DATE_FORMAT)


@task(retries=5, retry_delay_seconds=3 * 60)
def extract_siscan_laudos(
  email: str, password: str, opcao_exame: str, start_date: str, end_date: str
) -> pd.DataFrame:
  """
  Executa o scraper do SISCAN para coletar laudos em um intervalo de datas.
  """
  log(
    "(extract_siscan_laudos) iniciando coleta "
    f"opcao_exame={opcao_exame}, periodo={start_date} a {end_date}"
  )
  pacientes = run_scraper(
    email=email,
    password=password,
    opcao_exame=opcao_exame,
    start_date=start_date,
    end_date=end_date,
    headless=True,
  )
  df = pd.DataFrame(pacientes)

  if df.empty:
    log("(extract_siscan_laudos) nenhum registro retornado pelo scraper", level="warning")
    return df

  df["data_extracao"] = now_str()
  df = df.drop_duplicates(ignore_index=True)
  log(f"(extract_siscan_laudos) dataframe criado com {df.shape[0]} linha(s)")
  return df


@task
def parse_date(value: str) -> datetime:
  return datetime.strptime(value, DATE_FORMAT)


@task
def generate_extraction_windows(
  start_date: date | datetime, end_date: date | datetime | None, interval: int
) -> list[tuple[str, str]]:
  if interval < 1:
    raise ValueError("interval deve ser maior ou igual a 1")

  if isinstance(start_date, datetime):
    start_date = start_date.date()
  if isinstance(end_date, datetime):
    end_date = end_date.date()
  if end_date is None:
    end_date = today()

  windows = []
  current_start = start_date
  while current_start <= end_date:
    current_end = min(current_start + timedelta(days=interval - 1), end_date)
    windows.append((_format_date(current_start), _format_date(current_end)))
    current_start = current_end + timedelta(days=1)

  log(f"(generate_extraction_windows) {len(windows)} janela(s) gerada(s): {windows}")
  return windows


@task
def build_operator_parameters(
  windows: list[tuple[str, str]],
  bq_table: str,
  bq_dataset: str,
  opcao_exame: str,
  environment: str = "dev",
) -> list[dict[str, Any]]:
  return [
    {
      "environment": environment,
      "data_inicial": start,
      "data_final": end,
      "bq_dataset": bq_dataset,
      "bq_table": bq_table,
      "opcao_exame": opcao_exame,
    }
    for start, end in windows
  ]
