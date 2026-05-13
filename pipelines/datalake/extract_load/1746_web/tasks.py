# -*- coding: utf-8 -*-
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def foo(text:str) -> str:
  log("foo")

  return f"{text}"