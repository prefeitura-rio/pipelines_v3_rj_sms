# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
import asyncio
import importlib.util

from pathlib import Path
import unicodedata

from prefect.docker import DockerImage
from prefect.flows import Flow
from prefect.schedules import Schedule


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


async def get_deployable_files(raw_pipeline_filter: str, commit_sha: str):
	# Se recebemos um filtro de deploy
	if raw_pipeline_filter:
		# Trata esse filtro para ser uma pasta "pipelines/(...)"
		pipeline_filter = Path(
			"pipelines/" + (
				raw_pipeline_filter
				.replace(".", "")
				.removeprefix("pipelines")
				.removeprefix("/")
			)
		)
		return get_flows_for_paths([pipeline_filter])

	# Caso contrário (e opção padrão), temos que descobrir qual
	# deploy fazer; conferimos arquivos modificados
	changed_dirs = await get_changed_pipelines(commit_sha)
	if not changed_dirs:
		logging.info("Nenhum flow modificado; não há deploy a fazer")
		sys.exit(0)
	return get_flows_for_paths(changed_dirs)


def load_module_from_filename(module_name: str, filename: str):
	spec = importlib.util.spec_from_file_location(module_name, filename)
	module = importlib.util.module_from_spec(spec)
	sys.modules[module_name] = module
	spec.loader.exec_module(module)
	return module


async def main():
	# CWD: /home/runner/work/pipelines_v3_rj_sms/pipelines_v3_rj_sms

	environment = os.getenv("ENVIRONMENT", "dev")
	commit_sha = os.getenv("GITHUB_SHA", "HEAD")
	pipeline_filter = os.getenv("PIPELINE_FILTER", "")
	# batch_size = os.getenv("BATCH_SIZE", "3")
	# max_retries = os.getenv("MAX_RETRIES", "2")

	PREFECT_API_URL = os.getenv("PREFECT_API_URL")
	if not PREFECT_API_URL:
		raise ValueError("PREFECT_API_URL não foi definido!")
	PREFECT_API_KEY = os.getenv("PREFECT_API_KEY")
	if not PREFECT_API_KEY:
		raise ValueError("PREFECT_API_KEY não foi definido!")

	# Descobre quais arquivos precisam de deploy:
	# - Se recebeu um filtro (=pasta) de pipelines,
	#   faz deploy de todos os flows que passam pelo filtro
	# - Caso contrário, confere se há flows modificados
	#   nesse commit
	deployable_files = await get_deployable_files(
		pipeline_filter,
		commit_sha
	)
	logging.info(
		f"Encontrado(s) {len(deployable_files)} arquivo(s) para deploy: "
		f"{[ str(y) for y in deployable_files ]}"
	)

	# Carrega dinamicamente módulo Python do pipelines; ele é
	# necessário porque abaixo vamos carregar os flows como módulos,
	# e eles dependem de `pipelines`
	load_module_from_filename("pipelines", "./pipelines/__init__.py")
	logging.debug("pipelines carregado como módulo")

	# TODO: paralelizar
	for file in deployable_files:
		# Exemplo:
		# era  pipelines/datalake/transform/dbt/flows.py
		# vira pipelines.datalake.transform.dbt.flows
		module_name = file.as_posix().removesuffix(".py").replace("/", ".")
		logging.debug(f"carregando '{module_name}' como módulo")
		module = load_module_from_filename(module_name, file.as_posix())
		logging.debug(dir(module))

		# Encontra a variável `_flows` do arquivo flows.py
		if not hasattr(module, "_flows"):
			raise ValueError(f"Arquivo '{file.as_posix()}' não possui lista de flows em `_flows`")
		flows = getattr(module, "_flows")
		flows: list[Flow]

		# Encontra a variável `_schedules` do arquivo flows.py
		if not hasattr(module, "_schedules"):
			logging.warning(f"Arquivo '{file.as_posix()}' não possui lista de schedules em `_schedules`")
		schedules = getattr(module, "_schedules", [])  # pode não haver schedule
		schedules: list[Schedule]

		# TODO: conferir se existe variável _dockerfile,
		# se sim, tratar como caminho para Dockerfile do flow

		logging.debug(
			f"'{file}': encontrados {len(flows)} flow(s) "
			f"e {len(schedules)} schedule(s)"
		)
		# Para cada flow definido no arquivo (provavelmente 1 só)
		for flow in flows:
			# Normaliza o nome para deploy
			normalized_flow_name = re.sub(
				r"[^a-z_\-]",
				"",
				(
					unicodedata.normalize("NFD", flow.name)
					.lower()
					.replace(" ", "_")
				)
			)
			if len(normalized_flow_name) < 1:
				raise ValueError(f"Nome do flow '{flow.name}' é inválido!")
			
			if environment == "dev":
				normalized_flow_name += "_staging"

			# TODO: paralelizar (`for` externo)
			logging.debug(f"Fazendo deploy feito para .../{normalized_flow_name}")
			await flow.adeploy(
				name=flow.name,
				work_pool_name="gcp-wp",  # FIXME: não gosto que seja hardcoded assim
				image=DockerImage(
					name=f"ghcr.io/prefeitura-rio/pipelines_v3_rj_sms/{normalized_flow_name}",
					tag="latest",
					# dockerfile= TODO: variável _dockerfile mencionada acima
				),
				schedules=([] if environment == "dev" else schedules)
			)


if __name__ == "__main__":
	asyncio.run(main())
