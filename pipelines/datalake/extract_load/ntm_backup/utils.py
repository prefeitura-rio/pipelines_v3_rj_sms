# -*- coding: utf-8 -*-
from pipelines.utils.datetime import now


def generate_filename(prefix: str, database: str):
  timestamp = now().strftime("%Y%m%d_%H%M%S")
  return f"{prefix}_{database}_{timestamp}.sql.gz"
