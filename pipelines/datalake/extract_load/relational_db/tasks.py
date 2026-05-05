# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.utils.datetime import from_relative_date, now
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


# TODO: mover para utils? dentro da task?
def get_datetime_working_range(
  start_datetime: str = None,
  end_datetime: str = None,
):
  raise NotImplementedError("falta migrar :)")
  # something something return (start_datetime.isoformat(), end_datetime.isoformat())


@task(retries=3, retry_delay_seconds=30)
def download_from_db(
  db_url: str,
  db_schema: str,
  db_table: str,
  relative_date: str,
  historical_mode: bool,
  reference_datetime_column: str,
) -> None:
  query = f"SELECT * FROM {db_schema}.{db_table}"

  # TODO: renomear "historical_mode"
  if not historical_mode:
    (interval_start, interval_end) = get_datetime_working_range(
      start_datetime=from_relative_date(relative_date),
    )
    query += (
      f" WHERE {reference_datetime_column}"
      f" BETWEEN '{interval_start}' AND '{interval_end}'"
    )

  log(query)
  table = pd.read_sql(query, db_url)
  table["loaded_at"] = now()
  log(f"{len(table)} linhas baixadas")
  return table
