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

# Fornecedores que enviam o laudo PDF diretamente como base64 no campo `exame_resultado_laudo`.
# Para esses fornecedores, o pipeline extrai o PDF, faz upload para o GCS e substitui o campo
# pelo URI gs://. Os demais fornecedores enviam o laudo via endpoint separado (não base64).
FORNECEDORES_COM_LAUDO_BASE64 = {
  "da752b5f-56b1-4dd3-a9af-6d77a1f53b2a"  # Blessing
}
