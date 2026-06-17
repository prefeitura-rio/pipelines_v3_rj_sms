# -*- coding: utf-8 -*-
import pandas as pd
from prefect.client.schemas import StateType

from pipelines.utils.cleanup import prettify_duration
from pipelines.utils.env import get_prefect_url
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_discord_message
from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.prefect import get_flow_runs_with_state, set_flow_run_state


@task
def detect_running_flows() -> pd.DataFrame:
  """
  Obtém lista de flows em execução, e filtra por aqueles que estão
  em execução há pelo menos 5 minutos. Retorna DataFrame com dados
  sobre os flows.
  """
  flow_runs = get_flow_runs_with_state(states=["PENDING", "RUNNING"])
  data = []
  for obj in flow_runs:
    flow_run = obj["flow_run"]
    flow = obj["flow"]
    data.append(
      {
        "id": str(flow_run.id),
        "name": flow_run.name,
        "state": flow_run.state_name,
        "start_time": flow_run.expected_start_time,
        "version": flow_run.deployment_version,
        "flow_id": str(flow.id),
        "flow_name": flow.name,
      }
    )

  if len(data) == 0:
    return None

  result = pd.DataFrame(data)

  result["composed_name"] = result["flow_name"] + " -> " + result["name"]
  result["start_time"] = pd.to_datetime(
    result["start_time"], format="mixed"
  ).dt.tz_convert("America/Sao_Paulo")
  result["beginning_datetime"] = result["start_time"].apply(
    lambda x: x.strftime("%d/%m/%Y %H:%M:%S")
  )
  result["duration_seconds"] = result["start_time"].apply(
    lambda x: (pd.Timestamp.now(tz="America/Sao_Paulo") - x).total_seconds()
  )

  URL = get_prefect_url()
  result["flow_run_url"] = result["id"].apply(lambda x: f"{URL}/runs/flow-run/{x}")

  def classify(duration_seconds):
    if duration_seconds > 24 * 60 * 60:
      return "long"
    if duration_seconds > 12 * 60 * 60:
      return "warning"
    if duration_seconds >= 5 * 60:
      return "normal"
    # Não temos interesse em flows em execução há menos de 5min
    return "deleteme"

  result["classification_type"] = result["duration_seconds"].apply(classify)
  # Apaga todos os flows com <5min de duração
  result.drop(result[result["classification_type"] == "deleteme"].index, inplace=True)
  # Aqui é bastante possível que tenhamos filtrado todos os flows
  # encontrados; então novamente confere o tamanho do DataFrame e
  # retorna None caso esteja vazio
  if result.size == 0:
    return None

  def classify_emoji(duration_seconds):
    if duration_seconds > 24 * 60 * 60:
      return "☠️"
    if duration_seconds > 12 * 60 * 60:
      return "⚠️"
    return ""

  result["classification_emoji"] = result["duration_seconds"].apply(classify_emoji)

  # Duração legível
  result["duration"] = result["duration_seconds"].apply(
    lambda x: prettify_duration(x * 1000, precision="s")
  )

  return result


@task
def report_flows(running_flows: pd.DataFrame):
  """
  Envia mensagem para o Discord com informações sobre
  flows em execução há muito tempo
  """
  if running_flows is None:
    log("Nenhum flow de longa duração encontrado")
    send_discord_message(
      title="✅ Execuções em Andamento",
      message="Nenhum flow em execução há mais de 5min",
      slug="warning",
    )
    return

  running_flows.sort_values(by="start_time", ascending=True, inplace=True)

  flow_count = running_flows.shape[0]
  content = [
    (
      f"Foi detectado {flow_count} flow run:"
      if flow_count < 2
      else f"Foram detectados {flow_count} flow runs:"
    )
  ]
  for _, flow_run in running_flows.iterrows():
    name = flow_run["composed_name"]
    url = flow_run["flow_run_url"]
    duration = flow_run["duration"]
    beginning_datetime = flow_run["beginning_datetime"]
    emoji = flow_run["classification_emoji"]
    content.append(
      f"- {emoji} [**{name}**]({url}) :: {duration} desde {beginning_datetime}."
    )

  full_message = "\n".join(content)
  log(full_message)
  send_discord_message(
    title="⌛ Execuções em Andamento", message=full_message, slug="warning"
  )


@task
def cancel_flows(running_flows: pd.DataFrame):
  """
  Requisita o cancelamento de flows em execução há muito tempo
  """
  if running_flows is None:
    log("Nenhum flow de longa duração")
    return

  long_running_flows = running_flows[running_flows["classification_type"] == "long"]
  long_flows_count = long_running_flows.shape[0]
  if long_flows_count <= 0:
    log("Nenhum flow de longa duração")
    return

  log(f"Cancelando {long_flows_count} flow(s) de longa duração")

  full_message = [
    f"É {long_flows_count} execução longa em cancelamento:"
    if long_flows_count < 2
    else f"São {long_flows_count} execuções longas em cancelamento:"
  ]

  for _, flow_run in long_running_flows.iterrows():
    result = set_flow_run_state(
      flow_run_id=flow_run["id"],
      state=StateType.CANCELLING,
      message="Flow em execução há mais de 24h",
    )
    name = flow_run["composed_name"]
    url = flow_run["flow_run_url"]
    duration = flow_run["duration"]
    status = result["status"]
    # Status aqui é um de ACCEPT, REJECT, ABORT ou WAIT
    status_emoji = "✅" if status == "ACCEPT" else "❗"
    message = (
      f"- [**{name}**]({url}) de {duration} "
      f":: {status_emoji} ({status}/{result['details']})"
    )
    log(message)
    full_message.append(message)

  send_discord_message(
    title="☠️ Execuções Canceladas", message="\n".join(full_message), slug="warning"
  )
