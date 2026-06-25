# -*- coding: utf-8 -*-

from enum import Enum


class constants(Enum):

  RETENCAO_DIAS = 90

  GCS_PREFIX = {"daily": "mysql/daily", "weekly": "mysql/weekly"}

  NTM_CREDENTIALS_PATH = "/tmp/ntm_credentials.json"
