# -*- coding: utf-8 -*-
import asyncio
from typing import List

from pipelines.utils.logger import log


async def async_run_command(command: List[str], raise_on_error: bool = True):
	"""Inicia um subprocesso executando o comando especificado"""
	try:
		process = await asyncio.create_subprocess_exec(
			*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
		)
		stdout, stderr = await process.communicate()

		if process.returncode != 0:
			stderr_text = stderr.decode().strip()
			log(f"Comando `{' '.join(command)}` falhou: {stderr_text}", level="error")
			if raise_on_error:
				raise RuntimeError(stderr_text)
			return ""

		stdout_text = stdout.decode().strip()
		log(f"Comando: `{' '.join(command)}`", level="debug")
		log(f"STDOUT: {stdout_text}", level="debug")

		return stdout_text
	except Exception as e:
		log(f"Erro executando comando: {e}", level="error")
		if raise_on_error:
			raise e
		return ""


def run_command(command: List[str], raise_on_error: bool = True):
	"""Inicia um subprocesso executando o comando especificado"""
	return asyncio.run(async_run_command(command, raise_on_error=raise_on_error))
