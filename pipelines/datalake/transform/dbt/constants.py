# -*- coding: utf-8 -*-
from enum import Enum

from pipelines.constants import CIT, SUBGERAL, SUBPAV


class constants(Enum):
  GCS_BUCKET = {"prod": "rj-sms_dbt", "dev": "rj-sms-dev_dbt"}

  # Chaves equivalem ao $DBT_USER de cada desenvolvedor,
  # com todas as letras em minúsculas
  OWNERS = {
    ## CIT
    "cit": CIT.CIT_ID.value,
    "avellar": CIT.AVELLAR_ID.value,
    "herian": CIT.HERIAN_ID.value,
    "daniellira": CIT.DANIEL_ID.value,
    "karen": CIT.KAREN_ID.value,
    "pedro": CIT.PEDRO_ID.value,
    ## Outros
    "dayaners": SUBPAV.DAYANE_ID.value,
    "miloskimatheus": SUBGERAL.MILOSKI_ID.value,
  }
