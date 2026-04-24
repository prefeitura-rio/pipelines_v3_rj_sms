# -*- coding: utf-8 -*-
import asyncio
import selectors
import subprocess
import sys
from typing import IO, Dict, List, TextIO, cast

from pipelines.utils.logger import log


async def async_run_command(
  command: List[str],
  raise_on_error: bool = True,
  print_stdout: bool = True,
  print_stderr: bool = True,
) -> bool:
  """
  Inicia um subprocesso executando o comando especificado, recebido como
  lista de strings (ex.: [ "git", "diff", "HEAD^" ]). Opcionalmente pode
  disparar erro em caso de erros no subprocesso (`raise_on_error=True`,
  padrão), ou não. Retorna booleana, `True` se o processo foi bem sucedido,
  `False` se houve erro.
  """
  try:
    # [Ref] https://stackoverflow.com/a/79040308
    sub = subprocess.Popen(
      command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    assert sub.stdout and sub.stderr

    streams: Dict[IO[str], IO[str]] = {sub.stdout: sys.stdout, sub.stderr: sys.stderr}

    with selectors.DefaultSelector() as selector:
      for sub_stream, sys_stream in streams.items():
        selector.register(sub_stream, selectors.EVENT_READ, sys_stream)

      while streams:
        for selected, _ in selector.select():
          sub_stream = cast(IO[str], selected.fileobj)
          if sub_stream not in streams:
            continue

          line = sub_stream.readline()
          if not line:
            streams.pop(sub_stream)
            continue

          sys_stream: TextIO = selected.data
          sys_stream.write(line)
          sys_stream.flush()

          if sys_stream is sys.stdout:
            if print_stdout:
              log(f"[subproc.STDOUT] {line}")
          elif print_stderr:
            log(f"[subproc.STDERR] {line}", level="error")

    exit_code = sub.wait()

    if exit_code != 0:
      log(f"Comando `{' '.join(command)}` falhou", level="error")
      if raise_on_error:
        raise RuntimeError()
      return False

    log(f"Comando: `{' '.join(command)}` terminou", level="debug")
    return True
  except Exception as e:
    log(f"Erro executando comando: {e}", level="error")
    if raise_on_error:
      raise e
    return False


def run_command(
  command: List[str],
  raise_on_error: bool = True,
  print_stdout: bool = True,
  print_stderr: bool = True,
) -> bool:
  """Inicia um subprocesso executando o comando especificado"""
  return asyncio.run(
    async_run_command(
      command,
      raise_on_error=raise_on_error,
      print_stdout=print_stdout,
      print_stderr=print_stderr,
    )
  )
