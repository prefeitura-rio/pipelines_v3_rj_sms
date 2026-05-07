# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.utils.datetime import from_relative_date, now
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task(retries=3, retry_delay_seconds=30)
def download_from_db(
  db_url: str,
  db_schema: str,
  db_table: str,
  relative_date: str,
  extract_whole_table: bool,
  reference_datetime_column: str,
) -> None:
  """
  Executa um `SELECT * FROM {db_schema}.{db_table}`,
  conectando-se ao banco em `db_url`.

  Se `extract_whole_table=False`, adiciona um filtro
  `WHERE {reference_datetime_column} BETWEEN ...` que restringe
  o intervalo dos dados recuperados, com base no parâmetro
  `relative_date`. Este recebe valores no formato:

  * `D-x`: entre hoje e hoje menos `x` dias
  * `M-x`: entre hoje e o primeiro dia do mês atual menos `x` meses
  * `Y-x`: entre hoje e o primeiro dia do ano atual menos `x` anos
  """
  query = f"SELECT * FROM {db_schema}.{db_table}"

  if not extract_whole_table:
    interval_start = from_relative_date(relative_date)
    interval_end = now().isoformat()
    query += (
      f" WHERE {reference_datetime_column}"
      f" BETWEEN '{interval_start}' AND '{interval_end}'"
    )

  log(f"Executando query:\n{query}")
  table = pd.read_sql(query, db_url)
  table["loaded_at"] = now()
  log(f"Obtidas {len(table)} linhas da tabela '{db_schema}.{db_table}'")
  return table
