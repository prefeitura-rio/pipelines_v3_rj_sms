# -*- coding: utf-8 -*-,
from enum import Enum


class constants(Enum):
  API_URL = "https://sisreg-es.saude.gov.br"

  SCROLL_TIMEOUT = "2m"
  """
  Ao fazer uma consulta, a paginação é na forma de 'scrolls'.
  Criar um scroll congela os dados que existiam naquele instante.
  Essa constante é a duração pela qual requisitamos que esse
  'congelamento' seja persistido.
  """

  CONCURRENCY_LIMIT_TAG = "sisreg-extracao-paralela"
  """
  Nome da tag do Prefect, configurada via UI, que limita execuções
  concorrentes da task de extração
  """
