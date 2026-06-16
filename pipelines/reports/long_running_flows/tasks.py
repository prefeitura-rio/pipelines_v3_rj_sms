# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.utils.cleanup import prettify_duration
from pipelines.utils.env import get_prefect_url
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_discord_message
from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.prefect import get_flow_runs_with_state


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
    duration = prettify_duration(flow_run["duration_seconds"] * 1000)
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
  return


@task
def cancel_flows(running_flows: pd.DataFrame):
  """
  Cancels a list of long-running flow runs.
  This function takes a list of dictionaries, where each dictionary represents
  a flow run with at least an 'id' key. It attempts to cancel each flow run
  and logs the progress and results.
  Args:
    running_flows (list[dict]): A list of dictionaries, each containing
                     information about a long-running flow run.
                     Each dictionary must have an 'id' key.
  Returns:
    None
  """
  log("TODO: Não implementado", level="warning")
  # if running_flows is None:
  #   log("No running flows detected.")
  #   return

  # long_running_flows = running_flows[running_flows["classification_type"] == "long"]
  # if long_running_flows.shape[0] == 0:
  #   log("No long running flows detected.")
  #   return

  # log(f"Cancelling {long_running_flows.shape[0]} long running flows.")

  # full_message = [f"São {long_running_flows.shape[0]} execuções longas em cancelamento:"]

  # for _, flow_run in long_running_flows.iterrows():
  #   success = cancel_flow_run.run(flow_run_id=flow_run["id"])
  #   success_emoji = "✅" if success else "❌"
  #   message = f"- [**{flow_run['composed_name']}**]({flow_run['flow_run_url']}) de {flow_run['duration_seconds']:.1f} minutos :: Sucesso {success_emoji}"  # noqa
  #   full_message.append(message)
  #   log(message)

  # send_discord_message(
  #   title="☠️ Execuções Canceladas", message="\n".join(full_message), slug="warning"
  # )
  return
