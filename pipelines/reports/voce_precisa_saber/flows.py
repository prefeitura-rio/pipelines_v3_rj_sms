# -*- coding: utf-8 -*-
from typing import List, Optional, Union

from prefect.futures import wait

from pipelines.constants import CIT
from pipelines.datalake.extract_load.diario_oficial_rj.flows import (
  extract_diario_oficial_rj,
)
from pipelines.datalake.extract_load.diario_oficial_uniao.flows import dou_extraction
from pipelines.datalake.extract_load.tribunal_de_contas_rj.flows import (
  extract_tribunal_de_contas_rj,
)
from pipelines.datalake.transform.dbt.flows import sms_execute_dbt
from pipelines.utils.datalake import get_email_recipients_task
from pipelines.utils.infisical import get_secret_task
from pipelines.utils.prefect import (
  create_flow_run,
  flow,
  flow_config,
  wait_for_flow_run_task,
)

from .constants import constants as flow_constants
from .schedules import schedules
from .tasks import build_email, fetch_tcm_cases, get_todays_tcm_from_gcs, send_email


@flow(name="Report: CDI–Você Precisa Saber", owners=[CIT.AVELLAR_ID.value], tags=["CIT"])
def flow_voce_precisa_saber(
  date: Optional[str] = None,
  skip_to_email: bool = False,
  override_recipients: Optional[Union[str, List[str]]] = None,
  environment: str = "dev",
):
  #  ┌(1) Flow────────┐     ┌(2) Flow──────────┐
  #  │  Extração DOU  ├──┬──┼  Extração DO-RJ  │
  #  └────────────────┘  │  └──────────────────┘
  #                      ▼
  #          ┌(3) dbt─────────────────┐
  #          │  Filtro de relevância  │
  #          └───────────┬────────────┘
  #                      └───────►┌(4) Flow───────────┐
  #                      ┌────────┼  Extração do TCM  │
  #                      ▼        └───────────────────┘
  #                 ┌(5) Task─┐
  #                 │  Email  │
  #                 └─────────┘      [via asciiflow.com]

  # Caso o flow já tenha extraído os dados anteriormente, o parâmetro `skip_to_email`
  # permite que a extração inteira seja pulada, e só a etapa de envio de email seja
  # re-executada
  if not skip_to_email:
    ## (1) DOU
    dou_flow_run = create_flow_run(
      flow=dou_extraction,
      parameters={"environment": environment, "date": date},
      environment=environment,
    )

    ## (2) DO-RJ
    dorj_flow_run = create_flow_run(
      flow=extract_diario_oficial_rj,
      parameters={"environment": environment, "date": date},
      environment=environment,
    )

    ## Espera por (1) e (2)
    diario_wait_futures = []
    for fr_diario in [dou_flow_run, dorj_flow_run]:
      diario_wait_futures.append(wait_for_flow_run_task.submit(flow_run_id=fr_diario.id))
    wait(diario_wait_futures)

    ## (3) dbt
    # Queremos executar o seguinte comando:
    # $ dbt build --select +tag:cdi_vps+ --target ENV
    fr_dbt = create_flow_run(
      flow=sms_execute_dbt,
      parameters={
        "environment": environment,
        "rename_flow": True,
        "send_discord_report": False,
        "command": "build",
        "select": "+tag:cdi_vps+",
        "exclude": None,
        "target": None if environment == "prod" else "ci",
        "flag": None,
      },
      environment=environment,
    )

    ## Espera por (3)
    wait_dbt = wait_for_flow_run_task(flow_run_id=fr_dbt.id, timeout_seconds=(20 * 60))

    ## (4) TCM
    # Espera DBT terminar, pega casos da tabela
    tcm_cases = fetch_tcm_cases(environment=environment, date=date, wait_for=[wait_dbt])
    # Para cada caso, cria uma flow run de extração do caso, adiciona na lista
    tcm_wait_futures = []
    for tcm_case in tcm_cases:
      fr_tcm = create_flow_run(
        flow=extract_tribunal_de_contas_rj,
        parameters={"environment": environment, "case_id": tcm_case},
        environment=environment,
      )
      tcm_wait_futures.append(
        wait_for_flow_run_task.submit(flow_run_id=fr_tcm.id, timeout_seconds=(20 * 60))
      )
    # Espera por todos os flow runs do TCM
    wait(tcm_wait_futures)

  ## (5) Email
  URL = get_secret_task(
    secret_name=flow_constants.EMAIL_URL.value,
    path=flow_constants.EMAIL_PATH.value,
    environment=environment,
  )
  TOKEN = get_secret_task(
    secret_name=flow_constants.EMAIL_TOKEN.value,
    path=flow_constants.EMAIL_PATH.value,
    environment=environment,
  )
  df = get_todays_tcm_from_gcs(environment=environment, skipped=skip_to_email)
  (edition, error, message) = build_email(environment=environment, date=date, tcm_df=df)
  recipients = get_email_recipients_task(
    environment=environment,
    dataset="brutos_sheets",
    table="cdi_destinatarios",
    recipients=override_recipients,
    error=error,
  )
  send_email(
    date=date,
    api_base_url=URL,
    token=TOKEN,
    recipients=recipients,
    edition=edition,
    message=message,
  )


_flows = [flow_config(flow=flow_voce_precisa_saber, schedules=schedules)]
