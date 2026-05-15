# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from pipelines.constants import constants as global_consts
from pipelines.datalake.extract_load.prontua_rio.constants import (
  constants as prontuario_constants,
)
from pipelines.datalake.extract_load.prontua_rio.tasks import (
  build_openbase_parameters,
  build_postgres_parameters,
  create_temp_folders,
  delete_temp_folders,
  extract_openbase_data,
  extract_postgres_data,
  get_file,
  list_files_from_bucket,
  unpack_files,
  generate_current_folder,
)
from pipelines.utils.prefect import flow, flow_config, rename_flow_run, create_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.logger import log
from .schedules import schedules


######################################################################################
#                                 OPERATORS
######################################################################################


@flow(
  name="DataLake - Extração e Carga de Dados - ProntuaRio OpenBase",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.HERIAN_ID.value],
)
def prontuario_openbase_operator(
  cnes: str,
  blob_path: str,
  folder: str,
  environment: str = "dev",
  bucket_name: str = "subhue_backups",
  dataset: str = "brutos_prontuario_prontuaRio_staging",
  lines_per_chunk: int = 5_000,
):
  # rename_flow_run(new_name=f"{folder} - {cnes}")

  # 1 - Cria diretórios temporários
  create_temp_folders(
    folders=[
      prontuario_constants.UPLOAD_PATH.value,
      prontuario_constants.DOWNLOAD_DIR.value,
      prontuario_constants.UNCOMPRESS_FILES_DIR.value,
    ]
  )

  # 2 - Faz o download do arquivo OpenBase
  openbase_file = get_file(
    path=prontuario_constants.DOWNLOAD_DIR.value,
    bucket_name=bucket_name,
    environment=environment,
    blob_path=blob_path,
    blob_type="openbase",
  )

  # 3 - Descompressão dos arquivos
  unpacked_openbase = unpack_files(
    tar_files=openbase_file,
    output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
    files_to_extract=prontuario_constants.SELECTED_BASE_FILES.value,
    exclude_origin=True,
  )

  # 4 - Extração e upload (por chunk) das tabelas selecionadas dos arquivos OpenBase
  extract_openbase_data(
    data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
    output_dir=prontuario_constants.UPLOAD_PATH.value,
    cnes=cnes,
    environment=environment,
    dataset_id=dataset,
    lines_per_chunk=lines_per_chunk,
    tables_to_extract=prontuario_constants.SELECTED_OPENBASE_TABLES.value,
  )

  # 5 - Deletar arquivos e diretórios
  delete_temp_folders(
    folders=[
      prontuario_constants.DOWNLOAD_DIR.value,
      prontuario_constants.UNCOMPRESS_FILES_DIR.value,
      prontuario_constants.UPLOAD_PATH.value,
    ]
  )


@flow(
  name="DataLake - Extração e Carga de Dados - ProntuaRio Postgre",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.HERIAN_ID.value],
)
def prontuario_postgres_operator(
  cnes: str,
  blob_path: str,
  folder: str,
  environment: str = "dev",
  bucket_name: str = "subhue_backups",
  dataset: str = "brutos_prontuario_prontuaRio_staging",
  lines_per_chunk: int = 1_000,
):
  # rename_flow_run(new_name=f"{folder} - {cnes}")

  # 1 - Cria diretórios temporários
  create_temp_folders(
    folders=[
      prontuario_constants.UPLOAD_PATH.value,
      prontuario_constants.DOWNLOAD_DIR.value,
      prontuario_constants.UNCOMPRESS_FILES_DIR.value,
    ]
  )

  # 2 - Download do tar com os arquivos POSTGRES
  postgres_file = get_file(
    path=prontuario_constants.DOWNLOAD_DIR.value,
    bucket_name=bucket_name,
    environment=environment,
    blob_path=blob_path,
    blob_type="sql",
  )

  # 3 - Descompressão do arquivo hospub.sql
  unpacked_hospub = unpack_files(
    tar_files=postgres_file,
    output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
    files_to_extract=["hospub.sql"],
    exclude_origin=True,
  )

  # 4 - Extração das tabelas do arquivo hospub.sql
  extract_postgres_data(
    data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
    output_dir=prontuario_constants.UPLOAD_PATH.value,
    lines_per_chunk=lines_per_chunk,
    dataset_id=dataset,
    cnes=cnes,
    environment=environment,
    sql_file="hospub.sql",
    target_tables=prontuario_constants.SELECTED_HOSPUB_TABLES.value,
    wait_for=unpacked_hospub,
  )

  # 5 - Deletar arquivos e diretórios
  delete_temp_folders(
    folders=[
      prontuario_constants.DOWNLOAD_DIR.value,
      prontuario_constants.UNCOMPRESS_FILES_DIR.value,
      prontuario_constants.UPLOAD_PATH.value,
    ]
  )


######################################################################################
#                                     MANAGER
######################################################################################


@flow(
  name="DataLake - Extração e Carga de Dados - ProntuaRio Backups (MANAGER)",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.HERIAN_ID.value],
)
def prontuario_extraction_manager(
  environment: str = "dev",
  bucket_name: str = "subhue_backups",
  rename_flow: str = None,
  dataset: str = "brutos_prontuario_prontuaRio_staging",
  folder: str = "",
  chunk_size: int = 1_000,
):
  current_folder = generate_current_folder(folder=folder)
  rename_flow_run(new_name=f"{current_folder} - {environment}")

  # 1 - Listar os arquivos no bucket
  last_files = list_files_from_bucket(
    environment=environment, bucket_name=bucket_name, folder=current_folder
  )

  # 2 - Criar os operators para cada CNES
  ## 2.1 Criar os parametros para cada flow
  openbase_params = build_openbase_parameters(
    last_files=last_files,
    folder=current_folder,
    bucket_name=bucket_name,
    dataset_id=dataset,
    environment=environment,
    chunk_size=chunk_size,
  )

  postgres_params = build_postgres_parameters(
    last_files=last_files,
    folder=current_folder,
    bucket_name=bucket_name,
    dataset_id=dataset,
    environment=environment,
    chunk_size=chunk_size,
  )

  # 2.2 Criar as flows runs para Openbase
  for param in openbase_params:
    create_flow_run(
      flow_name="DataLake - Extração e Carga de Dados - ProntuaRio OpenBase",
      parameters=param,
      environment=environment
    )

  # 2.3 Criar as flows runs para Postgres
  for param in postgres_params:
    create_flow_run(
      flow_name="DataLake - Extração e Carga de Dados - ProntuaRio Postgres",
      parameters=param,
      environment=environment
    )


_flows = [
  flow_config(flow=prontuario_extraction_manager, schedules=schedules, memory="small"),
  flow_config(flow=prontuario_openbase_operator, memory="large"),
  flow_config(flow=prontuario_postgres_operator, memory="large"),
]
