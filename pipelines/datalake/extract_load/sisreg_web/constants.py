# -*- coding: utf-8 -*-

from enum import Enum


class constants(Enum):
  INFISICAL_PATH = "/sisreg"
  INFISICAL_USERNAME = "SISREG_USER"
  INFISICAL_PASSWORD = "SISREG_PASSWORD"

  URL = "https://sisregiii.saude.gov.br"
  USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
  )
  ENDPOINTS = {
    "escala": {
      "endpoint": "/cgi-bin/cons_escalas",
      "params": {
        "radioFiltro": "cpf",
        "etapa": "EXPORTAR_ESCALAS",
        "ibge": "330455",
        "qtd_itens_pag": "50",
        "clas_lista": "ASC",
        "dataInicial": "",
        "dataFinal": "",
        "status": "",
        "pagina": "",
        "ordenacao": "",
        "coluna": "",
      },
    }
  }
