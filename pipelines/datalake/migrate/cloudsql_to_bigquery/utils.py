# -*- coding: utf-8 -*-
import os
from urllib.parse import quote_plus


def build_sqlserver_odbc_connection_string(
  host: str, username: str, password: str, database_name: str, port: int, driver: str
) -> str:
  return (
    f"DRIVER={{{driver}}};"
    f"SERVER={host},{port};"
    f"DATABASE={database_name};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
  )


def quote_odbc_connection_string(connection_string: str) -> str:
  return quote_plus(connection_string)


def build_sample_parquet_path(database_name: str, output_dir: str = "/tmp") -> str:
  safe_database_name = "".join(
    character if character.isalnum() or character in ("_", "-") else "_"
    for character in database_name
  )
  return os.path.join(output_dir, f"{safe_database_name}_sample.parquet")


def hide_password(connection_string: str) -> str:
  parts = []
  for part in connection_string.split(";"):
    if part.upper().startswith("PWD="):
      parts.append("PWD=***")
    else:
      parts.append(part)
  return ";".join(parts)
