# -*- coding: utf-8 -*-
from typing import Dict, Literal

RecursosRMD = Literal[
  "RECURSO_FICHA_TUBERCULOSE", "RECURSO_FICHA_VIOLENCIA", "RECURSO_EXAMES_LABORATORIAIS"
]

resource_to_table_map: Dict[RecursosRMD, str] = {
  "RECURSO_FICHA_TUBERCULOSE": "ficha_tuberculose",
  "RECURSO_FICHA_VIOLENCIA": "ficha_violencia",
  "RECURSO_EXAMES_LABORATORIAIS": "exames_laboratoriais",
}
