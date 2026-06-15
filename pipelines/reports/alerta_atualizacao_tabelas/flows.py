# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.reports.alerta_atualizacao_tabelas.schedules import (
  freshness_hci_schedule,
  freshness_tables_schedule,
)
from pipelines.reports.alerta_atualizacao_tabelas.tasks import (
  send_discord_alert,
  send_hci_discord_alert,
  verify_hci_last_episodes,
  verify_tables_freshness,
)
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change


@flow(
  name="Report: Alerta Atualização de Tabelas", owners=[CIT.HERIAN_ID.value], tags=["CIT"]
)
def report_alerta_atualizacao_tabelas(environment: str = "dev", table_ids: dict = {}):
  results = verify_tables_freshness(environment=environment, table_ids=table_ids)
  send_discord_alert(environment=environment, results=results)


@flow(
  name="Report: Alerta Atualização de Episódios Assistenciais - HCI",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.HERIAN_ID.value],
)
def report_alerta_atualizacao_hci(environment: str = "dev"):
  hci_results = verify_hci_last_episodes(environment=environment)
  send_hci_discord_alert(environment=environment, last_episodes=hci_results)


_flows = [
  flow_config(
    flow=report_alerta_atualizacao_tabelas,
    schedules=freshness_tables_schedule,
    memory="small",
  ),
  flow_config(
    flow=report_alerta_atualizacao_hci, schedules=freshness_hci_schedule, memory="small"
  ),
]
