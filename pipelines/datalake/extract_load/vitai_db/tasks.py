# -*- coding: utf-8 -*-

import pandas as pd

from pipelines.utils.datetime import from_relative_date
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def create_working_time_range(
  interval_start: str = None, interval_end: str = None, relative_date: str = ""
) -> tuple[pd.Timestamp, pd.Timestamp]:

  if relative_date:
    interval_start = from_relative_date(relative_date)

  if not interval_start:
    start = pd.Timestamp.now(tz="America/Sao_Paulo") - pd.Timedelta(days=7)
    log(f"Interval start not provided. Using {start}.", level="warning")
  else:
    start = pd.to_datetime(interval_start)

  if not interval_end:
    end = pd.Timestamp.now(tz="America/Sao_Paulo")
    log("Interval end not provided. Getting current timestamp.", level="warning")
  else:
    end = pd.to_datetime(interval_end)

  return start, end


@task(retries=3, retry_delay_seconds=120, timeout_seconds=1200)
def define_queries(
  db_url: str,
  schema_name: str,
  table_name: str,
  dt_column: str,
  interval_start: pd.Timestamp,
  interval_end: pd.Timestamp,
  batch_size: int,
) -> list[str]:
  """
  Generates a list of SQL queries to fetch data from a specified table within
      a given time interval in batches.
  """
  interval_start_str = interval_start.strftime("%Y-%m-%d %H:%M:%S")
  interval_end_str = interval_end.strftime("%Y-%m-%d %H:%M:%S")

  query = f"""
        select count(*) as row_count
        from {schema_name}.{table_name}
        where {dt_column} between '{interval_start_str}' and '{interval_end_str}'
    """
  log("Built query: \n" + query)

  df = pd.read_sql(query, db_url)
  row_count = df["row_count"].values[0]

  if row_count == 0:
    log("No data found for the given interval", level="warning")
    return []

  queries = []
  for i in range(0, row_count, batch_size):
    queries.append(
      f"""
                select *
                from {schema_name}.{table_name}
                where {dt_column} between '{interval_start_str}' and '{interval_end_str}'
                limit {batch_size} offset {i}
            """
    )

  return queries


@task(retries=3, retry_delay_seconds=120, timeout_seconds=300)
def run_query(db_url: str, query: str, partition_column: str) -> pd.DataFrame:
  log("Running query: \n" + query)

  df = pd.read_sql(query, db_url, dtype=str)
  log(f"Query executed successfully. Found {df.shape[0]} rows.")

  if "id" in df.columns:
    log("Detected `id` column in dataframe. Renaming to `gid`", level="warning")
    df.rename(columns={"id": "gid"}, inplace=True)

  now = pd.Timestamp.now(tz="America/Sao_Paulo")
  df["datalake_loaded_at"] = now

  # We shouldn't fail if the partition_column doesn't exist, or we can handle it
  if partition_column in df.columns:
    df["partition_date"] = pd.to_datetime(df[partition_column]).dt.date
  else:
    df["partition_date"] = now.date()

  return df
