# -*- coding: utf-8 -*-
from prefect import flow

from pipelines.reports.alerta_atualizacao_tabelas.tasks import (
  send_discord_alert,
  send_hci_discord_alert,
  verify_hci_last_episodes,
  verify_tables_freshness,
)


@flow
def report_alerta_atualizacao_tabelas(environment: str = "staging", table_ids: dict = {}):
  results = verify_tables_freshness(environment=environment, table_ids=table_ids)
  return send_discord_alert(environment=environment, results=results)


@flow
def report_alerta_atualizacao_hci(environment: str = "dev"):
  hci_results = verify_hci_last_episodes(environment=environment)
  return send_hci_discord_alert(environment=environment, last_episodes=hci_results)
