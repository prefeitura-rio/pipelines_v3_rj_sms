# -*- coding: utf-8 -*-
# /// script
# requires-python = ">=3.13"
# dependencies = []
# ///
import os
import logging
import asyncio

from pathlib import Path


logging.basicConfig(
	level=os.getenv("LOG_LEVEL", "INFO").upper(),
)


async def run_subprocess(command: list[str]) -> asyncio.subprocess.Process:
	"""Cria e executa um subprocesso"""
	return await asyncio.create_subprocess_exec(
		*command,
		stdout=asyncio.subprocess.PIPE,
		stderr=asyncio.subprocess.PIPE,
	)


async def get_changed_directories(sha: str, path_filter: Path) -> list[Path]:
	"""Lista arquivos modificados no commit especificado"""
	command = ["git", "diff", "--name-only", f"{sha}^", sha, "--", path_filter.as_posix()]

	try:
		process = await run_subprocess(command)
		stdout, stderr = await process.communicate()

		if process.returncode != 0:
				logging.warning(f"Git command failed: {stderr.decode().strip()}")
				return []

		parsed_stdout = stdout.decode().strip().splitlines()
		logging.debug(f"Command: `{' '.join(command)}`")
		logging.debug(f"STDOUT: {parsed_stdout}")

		# TODO: aqui não deveria ser um set()? pode ter repetido?
		return list({Path(f).parent for f in parsed_stdout if f.strip()})
	except Exception as e:
		logging.error(f"Failed to get changed directories: {e}")
		return []


async def main():
	logging.info(os.getcwd())

	environment = os.getenv("ENVIRONMENT", "dev")
	force_deploy = os.getenv("FORCE_DEPLOY", "0")
	commit_sha = os.getenv("GITHUB_SHA", "HEAD")
	# batch_size = os.getenv("BATCH_SIZE", "3")
	# max_retries = os.getenv("MAX_RETRIES", "2")
	pipeline_filter = os.getenv("PIPELINE_FILTER")

	changed_dirs = get_changed_directories(commit_sha, pipeline_filter)
	logging.info(changed_dirs)
