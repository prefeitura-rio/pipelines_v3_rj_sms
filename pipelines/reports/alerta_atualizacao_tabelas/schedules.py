# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

freshness_tables_flow_parameters = [
  {
    "environment": "prod",
    "table_ids": {
      "rj-sms.app_historico_clinico.episodio_assistencial": ["DIT - HCI"],
      "rj-sms.app_historico_clinico.paciente": ["DIT - HCI"],
      "rj-sms.projeto_gestacoes.linha_tempo": ["S/SUBPAV/SAP - BI de Gestantes"],
      "rj-sms.projeto_gestacoes.encaminhamentos": ["S/SUBPAV/SAP - BI de Gestantes"],
      "rj-sms.projeto_estoque.estoque_posicao_atual": ["DIT - BI de Farmácia"],
      "rj-sms.projeto_whatsapp.telefones_validos": ["IplanRio - E Aí?"],
    },
  }
]

freshness_hci_flow_parameters = [{"environment": "prod"}]

freshness_tables_schedule = create_schedule_list(
  parameters_list=freshness_tables_flow_parameters,
  interval="daily",
  config={"hour": 8, "minute": 0},
)

freshness_hci_schedule = create_schedule_list(
  parameters_list=freshness_hci_flow_parameters,
  interval="daily",
  config={"hour": 8, "minute": 0},
)
