# -*- coding: utf-8 -*-

import pandas as pd
from google.cloud import bigquery
from prefect import task

from pipelines.utils.datetime import SAO_PAULO_TZ, now
from pipelines.utils.monitor import send_discord_message


@task
def verify_tables_freshness(environment: str, table_ids: dict):
  results = []
  client = bigquery.Client()
  for table_id, projects in table_ids.items():
    table_metadata = client.get_table(table_id)

    today_americas = now()

    last_updated = (
      table_metadata.modified
      if table_metadata.modified is not None
      else table_metadata.created
    )
    last_updated_americas = last_updated.astimezone(SAO_PAULO_TZ)

    results.append(
      {
        "table_id": table_id,
        "last_updated": last_updated_americas,
        "was_updated_today": last_updated_americas.date() == today_americas.date(),
        "affected_projects": projects,
      }
    )

  return results


@task
def send_discord_alert(environment: str, results: list):
  lines = []
  affected_projects = []
  for result in results:
    if result["was_updated_today"]:
      continue

    affected_projects.extend(result["affected_projects"])

    last_updated = result["last_updated"].astimezone(SAO_PAULO_TZ)
    lines.append(
      f"- 🔴 `{result['table_id']}` *Última Modificação: {last_updated.strftime('%d/%m/%Y %H:%M:%S')}*"
    )

  if len(lines) == 0:
    send_discord_message(
      title="Alerta de Tabelas Desatualizadas",
      message="🟢 Todas as tabelas estão atualizadas",
      slug="freshness",
    )
    return

  affected_projects = list(set(affected_projects))
  affected_projects = [f"- {project}" for project in affected_projects]

  message = "**Houve interrupção na atualização das tabelas:**\n" + "\n".join(lines)
  message += "\n\n**Projetos Afetados:**\n" + "\n".join(affected_projects)

  send_discord_message(
    title="Alerta de Tabelas Desatualizadas", message=message, slug="freshness"
  )


@task
def verify_hci_last_episodes(environment: str) -> list:
  sql = """
        select  
            provider,
            max(entry_datetime) as last_episode
        from `rj-sms.app_historico_clinico.episodio_assistencial` 
        group by provider
        order by provider asc
    """
  client = bigquery.Client()
  df = client.query(sql).to_dataframe()
  df["last_episode"] = pd.to_datetime(df["last_episode"])
  last_episodes = [dict(df.iloc[row]) for row in range(0, len(df))]

  return last_episodes


@task
def send_hci_discord_alert(environment: str, last_episodes: list):
  lines = []

  for record in last_episodes:
    provider = record["provider"]
    last_episode = record["last_episode"].replace(tzinfo=SAO_PAULO_TZ)
    days_diff = (now() - last_episode).days

    if provider != "prontuaRio":
      if days_diff > 1:
        lines.append(
          f"- 🔴 `{provider}` *Último Episódio: {last_episode.strftime('%d/%m/%Y %H:%M:%S')}* ({days_diff} dias atrás)"
        )
      else:
        lines.append(
          f"- 🟢 `{provider}` *Último Episódio: {last_episode.strftime('%d/%m/%Y %H:%M:%S')}* ({days_diff} dias atrás)"
        )
    else:
      # A atualização dos dados do ProntuaRio são semanais via backups
      if days_diff > 8:
        lines.append(
          f"- 🔴 `{provider}` *Último Episódio: {last_episode.strftime('%d/%m/%Y %H:%M:%S')}* ({days_diff} dias atrás)"
        )
      else:
        lines.append(
          f"- 🟢 `{provider}` *Último Episódio: {last_episode.strftime('%d/%m/%Y %H:%M:%S')}* ({days_diff} dias atrás)"
        )

  send_discord_message(
    title="Atualização de Episódios Assistenciais - HCI",
    message="\n".join(lines),
    slug="freshness",
  )
