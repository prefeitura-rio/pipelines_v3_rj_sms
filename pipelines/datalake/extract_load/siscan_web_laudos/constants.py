# -*- coding: utf-8 -*-

from enum import Enum


class constants(Enum):
  INFISICAL_PATH = "/siscan_web"
  INFISICAL_USERNAME = "SISCAN_MAIL"
  INFISICAL_PASSWORD = "SISCAN_PASS"

  DEFAULT_DATASET_ID = "brutos_siscan_web"
  DEFAULT_EXAM_OPTION = "mamografia"
  DEFAULT_TABLE_ID = "laudos_mamografia"
  PARTITION_COLUMN = "data_extracao"
  SOURCE_FORMAT = "parquet"
