# -*- coding: utf-8 -*-
from typing import List

from pipelines.constants import constants as global_consts
from pipelines.utils.infisical import get_secret
from pipelines.utils.prefect import flow, flow_config
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.datalake import get_email_recipients_task

from .constants import informes_seguranca_constants
from .schedules import schedules
from .tasks import build_email, fetch_cids, send_email


@flow(
  name="Report: Informes de Segurança",
  state_handlers=[handle_flow_state_change],
  owners=[global_consts.AVELLAR_ID.value],
)
def report_informes_seguranca(
  date: str | None = None,
  override_recipients: List[str] | str | None = None,
  skip_email: bool = False,
  environment: str = "dev",
):
  """
  Args:
    date(str?):
      Data para buscar os atendimentos. Quando `None`, usa
      o dia de ontem.
    override_recipients(list[str] | str?):
      Um ou múltiplos endereços de email recipientes da
      mensagem. Quando `None`, usa os recipientes extraídos
      da planilha.
    skip_email(bool?):
      Flag para que o email não seja enviado. Útil durante
      desenvolvimento. Por padrão, é `False`.
    environment(str?):
      Ambiente de execução; "dev" por padrão.
  """

  results, fetch_error = fetch_cids(environment=environment, date=date)

  message, build_error = build_email(cids=results, date=date, error=fetch_error)

  recipients = get_email_recipients_task(
    environment=environment,
    dataset="brutos_sheets",
    table="seguranca_destinatarios",
    recipients=override_recipients,
    error=(fetch_error or build_error),
  )

  URL = get_secret(
    secret_name=informes_seguranca_constants.EMAIL_URL.value,
    path=informes_seguranca_constants.EMAIL_PATH.value,
    environment=environment,
  )
  TOKEN = get_secret(
    secret_name=informes_seguranca_constants.EMAIL_TOKEN.value,
    path=informes_seguranca_constants.EMAIL_PATH.value,
    environment=environment,
  )
  send_email(
    api_base_url=URL,
    token=TOKEN,
    message=message,
    recipients=recipients,
    error=(fetch_error or build_error),
    date=date,
    write_to_file_instead=skip_email,
  )


# num_workers=1, memory_request="2Gi", memory_limit="2Gi",
_flows = [flow_config(flow=report_informes_seguranca, schedules=schedules)]
