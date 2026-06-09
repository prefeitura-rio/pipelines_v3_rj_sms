# -*- coding: utf-8 -*-

from enum import Enum


class constants(Enum):
  INFISICAL_PATH = "/sisreg"
  INFISICAL_USERNAME = "SISREG_USER"
  INFISICAL_PASSWORD = "SISREG_PASSWORD"

  URL = "https://sisregiii.saude.gov.br"
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
      }
    }
  }
