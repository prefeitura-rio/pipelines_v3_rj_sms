# -*- coding: utf-8 -*-
# pylint: disable=C0103

from pipelines.constants import CIT
from pipelines.datalake.extract_load.diario_oficial_uniao.constants import (
    constants as flow_constants,
)
from pipelines.datalake.extract_load.diario_oficial_uniao.tasks import (
    create_dirs,
    delete_dirs,
    download_files,
    get_xml_files,
    login,
    parse_date,
    report_extraction_status,
    unpack_zip,
    upload_to_datalake,
)
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change

from .schedules import schedules


@flow(
    name="DataLake - Extração e Carga de Dados - Diário Oficial da União (API)",
    state_handlers=[handle_flow_state_change],
    owners=[CIT.HERIAN_ID.value],
)
def dou_extraction(
    environment: str = "dev",
    date: str = "",
    dou_section: str = "DO1 DO2 DO3",
    dataset_id: str = "brutos_diario_oficial",
):
    """
    Fluxo de extração e carga de atos oficiais do Diário Oficial da União (DOU).
    """

    create_dirs()

    parsed_date = parse_date(date=date)

    # Realiza o login
    session = login(enviroment=environment)

    # Faz o download dos arquivos .zip com os atos oficiais de cada seção
    zip_files = download_files(session=session, sections=dou_section, date=parsed_date)

    # Descompacta os arquivos .zip (Se não houver atos oficiais para descompactar, retorna False)
    # unpack_zip was returning boolean instead of None if it didn't extract.
    # The original flow assigned the result to extraction_status.
    unpack_zip(zip_files=zip_files, output_path=flow_constants.OUTPUT_DIR.value)

    # Pega as informações dos xml de cada ato oficial
    parquet_file = get_xml_files(xml_dir=flow_constants.OUTPUT_DIR.value, wait_for=unpack_zip)

    # Faz o upload para o bigquery
    upload_status = upload_to_datalake(parquet_path=parquet_file, dataset=dataset_id, wait_for=parquet_file)

    # Reportando status da extração
    report_status = report_extraction_status(status=upload_status, date=date, environment=environment, )

    # Deleta os diretórios temporários
    delete_dirs(wait_for=report_status)


_flows = [flow_config(flow=dou_extraction, schedules=schedules)]
