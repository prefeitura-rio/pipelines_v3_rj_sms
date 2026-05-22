# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import List, Optional, Union

from pipelines.constants import CIT
from pipelines.datalake.extract_load.diario_oficial_rj.flows import (
  extract_diario_oficial_rj,
)
from pipelines.utils.prefect import create_flow_run, flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_email_recipients, get_secret_key

from .constants import constants as flow_constants
from .tasks import build_email, fetch_tcm_cases, get_todays_tcm_from_gcs, send_email


@flow(
  name="Report: Você Precisa Saber/CDI",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.AVELLAR_ID.value],
)
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
    # dou_flow_run = create_flow_run(
    #   flow_= ... ,
    #   parameters={"environment": environment, "date": date},
    # )

    ## (2) DO-RJ
    dorj_flow_run = create_flow_run(
      flow_=extract_diario_oficial_rj,
      parameters={"environment": environment, "date": date},
    )

    ## Agrega (1) e (2)
    wait_dos = wait_for_flow_run.map(
      flow_run_id=[dou_flow_run, dorj_flow_run],
      stream_states=unmapped(True),
      stream_logs=unmapped(True),
      raise_final_state=unmapped(False),
      max_duration=unmapped(timedelta(minutes=60)),
    )

    ## (3) dbt
    dbt_params = create_dbt_params_dict(environment=environment)
    dbt_flow_run = create_flow_run(
      flow_name="DataLake - Transformação - DBT",
      project_name=project_name,
      parameters=dbt_params,
      labels=current_flow_run_labels,
      upstream_tasks=[wait_dos],
    )

    wait_dbt = wait_for_flow_run(
      flow_run_id=dbt_flow_run,
      stream_states=True,
      stream_logs=True,
      raise_final_state=True,
      max_duration=timedelta(minutes=20),
    )

    ## (4) TCM
    # Espera DBT terminar, pega casos da tabela
    tcm_cases = fetch_tcm_cases(
      environment=environment, date=date, upstream_tasks=[wait_dbt]
    )
    # Cria um dict() de parâmetros para cada caso da tabela
    tcm_params = create_tcm_params_dict.map(
      case_id=tcm_cases, environment=unmapped(environment)
    )

    tcm_flow_runs = create_flow_run.map(
      flow_name=unmapped(
        "DataLake - Extração e Carga de Dados - Tribunal de Contas do Município"
      ),
      project_name=unmapped(project_name),
      parameters=tcm_params,
      labels=unmapped(current_flow_run_labels),
    )
    wait_tcm = wait_for_flow_run.map(
      flow_run_id=tcm_flow_runs,
      stream_states=unmapped(True),
      stream_logs=unmapped(True),
      raise_final_state=unmapped(False),
      max_duration=unmapped(timedelta(minutes=20)),
    )

    ## (5) Email
    URL = get_secret_key(
      secret_path=flow_constants.EMAIL_PATH.value,
      secret_name=flow_constants.EMAIL_URL.value,
      environment=environment,
    )
    TOKEN = get_secret_key(
      secret_path=flow_constants.EMAIL_PATH.value,
      secret_name=flow_constants.EMAIL_TOKEN.value,
      environment=environment,
    )
    df = get_todays_tcm_from_gcs(
      environment=environment, skipped=False, upstream_tasks=[wait_tcm]
    )
    (edition, error, message) = build_email(environment=environment, date=date, tcm_df=df)
    recipients = get_email_recipients(
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

  ## Somente envio de email
  if skip_to_email:
    URL = get_secret_key(
      secret_path=flow_constants.EMAIL_PATH.value,
      secret_name=flow_constants.EMAIL_URL.value,
      environment=ENVIRONMENT,
    )
    TOKEN = get_secret_key(
      secret_path=flow_constants.EMAIL_PATH.value,
      secret_name=flow_constants.EMAIL_TOKEN.value,
      environment=ENVIRONMENT,
    )
    df = get_todays_tcm_from_gcs(environment=ENVIRONMENT, skipped=True)
    (edition, error, message) = build_email(environment=ENVIRONMENT, date=DATE, tcm_df=df)
    recipients = get_email_recipients(
      environment=ENVIRONMENT,
      dataset="brutos_sheets",
      table="cdi_destinatarios",
      recipients=OVERRIDE_RECIPIENTS,
      error=error,
    )
    send_email(
      date=DATE,
      api_base_url=URL,
      token=TOKEN,
      recipients=recipients,
      edition=edition,
      message=message,
    )


# _flows = [flow_config(flow=flow_voce_precisa_saber, schedules=[])]
