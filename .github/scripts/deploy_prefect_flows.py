# -*- coding: utf-8 -*-
# /// script
# requires-python = ">=3.13"
# dependencies = []
# ///
import os
import sys
import logging
import asyncio

from pathlib import Path


logging.basicConfig(
	level=os.getenv("LOG_LEVEL", "INFO").upper(),
	format="[%(levelname)s]\t%(message)s"
)


async def run_subprocess(command: list[str]) -> asyncio.subprocess.Process:
	"""Cria e executa um subprocesso"""
	return await asyncio.create_subprocess_exec(
		*command,
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.PIPE,
	)


async def get_changed_pipelines(sha: str) -> list[Path]:
	"""Lista pipelines com arquivos modificados no commit especificado"""
	command = ["git", "diff", "--name-only", f"{sha}^", sha, "--", "pipelines"]

	try:
		process = await run_subprocess(command)
		stdout, stderr = await process.communicate()

		if process.returncode != 0:
				logging.warning(f"Comando git falhou: {stderr.decode().strip()}")
				return []

		parsed_stdout = stdout.decode().strip().splitlines()
		logging.debug(f"Comando: `{' '.join(command)}`")
		logging.debug(f"STDOUT: {parsed_stdout}")

		return list(
			set(
				Path(f.strip()).parent
				for f in parsed_stdout
				if f.strip()
			)
		)
	except Exception as e:
		logging.error(f"Erro ao obter arquivos modificados: {e}")
		return []


def get_flows_for_paths(paths: list[Path]) -> list[Path]:
	# Encontra todos os flows.py nas pastas e subpastas dadas
	flows = []
	for path in paths:
		for file in list(path.rglob("flows.py")):
			flows.append(file)
	# Deduplica, remove flows.py da raiz
	return set(flows) - set([ Path("pipelines/flows.py") ])


async def main():
	# CWD: /home/runner/work/pipelines_v3_rj_sms/pipelines_v3_rj_sms

	environment = os.getenv("ENVIRONMENT", "dev")
	force_deploy = os.getenv("FORCE_DEPLOY", "0")
	commit_sha = os.getenv("GITHUB_SHA", "HEAD")
	# batch_size = os.getenv("BATCH_SIZE", "3")
	# max_retries = os.getenv("MAX_RETRIES", "2")
	raw_pipeline_filter = os.getenv("PIPELINE_FILTER", "")

	deployable_files = []
	# Se recebemos um filtro de deploy
	if raw_pipeline_filter:
		pipeline_filter = Path(
			"pipelines/" + (
				raw_pipeline_filter
				.replace(".", "")
				.removeprefix("pipelines")
				.removeprefix("/")
			)
		)
		deployable_files = get_flows_for_paths([pipeline_filter])
	# Caso contrário (e opção padrão), vemos se há deploy a fazer
	# nos arquivos modificados
	else:
		changed_dirs = await get_changed_pipelines(commit_sha)
		if not changed_dirs:
			logging.info("Nenhum flow modificado; não há deploy a fazer")
			sys.exit(0)
		deployable_files = get_flows_for_paths(changed_dirs)

	logging.info(
		f"Encontrados {len(deployable_files)} arquivos para deploy: "
		f"{[ str(y) for y in deployable_files ]}"
	)


if __name__ == "__main__":
	asyncio.run(main())
