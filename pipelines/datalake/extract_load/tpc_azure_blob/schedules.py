# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

from .constants import constants as tpc_constants


parameters = [
  {
    "blob_file": "posicao",
    "table_id": "estoque_posicao",
    "dataset_id": tpc_constants.DATASET_ID.value,
    "environment": "prod",
    "rename_flow": True,
  }
  # {
  #   "blob_file": "pedidos",
  #   "table_id": "estoque_pedidos_abastecimento",
  #   "dataset_id": tpc_constants.DATASET_ID.value,
  # },
  # {
  #   "blob_file": "recebimento",
  #   "table_id": "estoque_recebimento",
  #   "dataset_id": tpc_constants.DATASET_ID.value,
  # },
]

schedules = create_schedule_list(
  parameters_list=parameters, interval="daily", config={"hour": 5, "minute": 30}
)
