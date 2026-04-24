# -*- coding: utf-8 -*-
from pipelines.constants import constants as global_consts
from pipelines.utils.google import ensure_instance_running, ensure_instance_stopped
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import restore_gcs_backup_to_cloudsql


@flow(
  name="Migrate - GCS to Cloud SQL",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.DANIEL_ID.value],
  description="Restaura backups do GCS para uma instância Cloud SQL",
)
def gcs_to_cloudsql(
  items: list[dict] = [
    {
      "source_uri": "gs://bucket/path/file.bak",
      "database_name": "example_database",
      "metadata": {},
    }
  ],
  instance_name: str = "vitacare",
  environment: str = "dev",
) -> list[dict]:
  """
  Processa backups do GCS e restaura cada item em uma instância Cloud SQL.

  Args:
          items (list[dict]): Itens a serem restaurados.
          instance_name (str): Nome da instância Cloud SQL.
          environment (str, optional): Ambiente de execução do flow.

  Returns:
          list[dict]: Lista com o resultado do processamento de cada item.
  """

  if not items:
    log("(gcs_to_cloudsql) nenhum item para processar")
    return []

  results = []

  try:
    ensure_instance_running(instance_name=instance_name)

    for item in items:
      results.append(
        restore_gcs_backup_to_cloudsql(item=item, instance_name=instance_name)
      )

  finally:
    try:
      ensure_instance_stopped(instance_name=instance_name)
    except Exception as exc:
      log(
        f"(gcs_to_cloudsql) erro ao desligar instância '{instance_name}': {repr(exc)}",
        level="error",
      )

  total_success = sum(1 for result in results if result["status"] == "success")
  total_failed = sum(1 for result in results if result["status"] == "failed")
  log(
    f"(gcs_to_cloudsql) processamento finalizado: "
    f"{total_success} sucesso(s), {total_failed} falha(s)"
  )
  return results


_flows = [flow_config(flow=gcs_to_cloudsql)]
