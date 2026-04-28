# -*- coding: utf-8 -*-
from pipelines.utils import google
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .utils import build_gcs_to_cloudsql_result, validate_restore_input


@task
def restore_gcs_backup_to_cloudsql(item: dict, instance_name: str) -> dict:
  """
  Restaura um backup do GCS para uma instância do Cloud SQL.

  Args:
          item (dict): Item com os dados da restauração.
          instance_name (str): Nome da instância Cloud SQL de destino.

  Returns:
          dict: Resultado padronizado do processamento do item.
  """
  source_uri = item.get("source_uri")
  database_name = item.get("database_name")

  try:
    validate_restore_input(item)
    source_uri = item["source_uri"]
    database_name = item["database_name"]

    log(
      f"Iniciando restauração de '{source_uri}' para o banco "
      f"'{database_name}' na instância '{instance_name}'"
    )

    google.delete_database(instance_name=instance_name, database_name=database_name)
    google.wait_for_operations(instance_name=instance_name)

    google.import_backup_to_database(
      instance_name=instance_name, source_uri=source_uri, database_name=database_name
    )
    google.wait_for_operations(instance_name=instance_name)

    return build_gcs_to_cloudsql_result(item=item, status="success")

  except Exception as exc:  # pylint: disable=broad-except
    log(
      f"Erro restaurando backup '{source_uri}' para o banco "
      f"'{database_name}': {repr(exc)}",
      level="error",
    )
    return build_gcs_to_cloudsql_result(item=item, status="failed", error_detail=str(exc))
