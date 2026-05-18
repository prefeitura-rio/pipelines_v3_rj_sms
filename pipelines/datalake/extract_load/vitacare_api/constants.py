# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
  INFISICAL_PATH = "/prontuario-vitacare"
  INFISICAL_VITACARE_USERNAME = "USERNAME"
  INFISICAL_VITACARE_PASSWORD = "PASSWORD"

  ENDPOINT = {
    "posicao": "/reports/pharmacy/stocks",
    "movimento": "/reports/pharmacy/movements",
    "vacina": "/reports/vacinas/listagemvacina",
    "condicao": "/reports/attendances/attendanceparamcid",
  }

  DATASET_ID = "brutos_prontuario_vitacare"
