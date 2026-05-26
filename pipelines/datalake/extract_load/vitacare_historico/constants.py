# -*- coding: utf-8 -*-
from enum import Enum


class vitacare_constants(Enum):
  INFISICAL_PATH = "/prontuario-vitacare-database"
  INFISICAL_CONNECTION_NAME = "DATABASE_CONNECTION_NAME"
  INFISICAL_PORT = "DATABASE_PORT"
  INFISICAL_USERNAME = "DATABASE_USER"
  INFISICAL_PASSWORD = "DATABASE_PASSWORD"
  INFISICAL_TABLES = "DATABASE_HISTORICO_TABLES"

  LOCAL_DATABASE_HOST = "127.0.0.1"
  INSTANCE_NAME = "vitacare"
  CNES_CONCURRENCY_LIMIT = 10
  DESTINATION_DATASET = "brutos_prontuario_vitacare_historico"
  LOG_DATASET = "controle_pipelines"
  LOG_TABLE = "log_vitacare_historico"
