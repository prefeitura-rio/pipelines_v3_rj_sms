# -*- coding: utf-8 -*-
from pipelines.constants import CIT
from pipelines.utils.datetime import today_str
from pipelines.utils.prefect import flow, flow_config, rename_flow_run

from .schedules import schedule
from .tasks import cancel_flows, detect_running_flows, report_flows


@flow(name="Report: Flows de Longa Duração", owners=[CIT.CIT_ID.value], tags=["CIT"])
def report_long_running_flows(environment: str = "dev"):
  rename_flow_run(new_name=f"Longa duração (env={environment}) [{today_str()}]")
  long_running_flows = detect_running_flows()
  report_flows(running_flows=long_running_flows)
  cancel_flows(running_flows=long_running_flows)


_flows = [flow_config(flow=report_long_running_flows, schedules=schedule)]
