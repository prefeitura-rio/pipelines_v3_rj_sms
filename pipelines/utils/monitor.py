# -*- coding: utf-8 -*-
import asyncio
import os
from typing import List, Literal

import aiohttp
from discord import AllowedMentions, Embed, File, Webhook
from prefect.context import FlowRunContext, TaskRunContext

from pipelines.utils.env import get_current_environment, get_prefect_url
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log


async def send_discord_webhook(
	slug: Literal["dbt-runs", "data-ingestion", "warning", "hci_status"],
	text_content: str = None,
	embed_content: List[Embed] = None,
	file_path: str = None,
):
	"""
	Envia mensagem para um webhook do Discord. Ainda que
	`text_content` e `embed_content` sejam parâmetros opcionais,
	é necessário definir pelo menos um.

	Args:
		slug(str):
			Referência ao canal destino, usada para obter o URL do webhook
		text_content(str?):
			Conteúdo textual da mensagem
		embed_content(list[Embed]?):
			Conteúdo já formatado como `Embed`
		file_path(str?):
			Caminho de arquivo a anexar à mensagem; ex.: arquivo de logs em .txt
	"""
	if not slug:
		raise ValueError("É preciso informar um `slug`!")

	environment = get_current_environment()
	secret_name = f"DISCORD_WEBHOOK_URL_{slug.upper()}"
	webhook_url = get_secret(
		secret_name=secret_name, environment=environment, path="/discord"
	)

	if not text_content and not embed_content:
		raise ValueError("É preciso definir pelo menos um entre conteúdo textual e Embed")
	if text_content and len(text_content) > 2000:
		raise ValueError(
			f"Conteúdo textual é longo demais: possui tamanho {len(text_content)}, máximo é 2000"
		)
	if embed_content and len(embed_content) > 5:
		raise ValueError(
			f"Embeds demais: possui {len(embed_content)} entradas, máximo é 5"
		)

	params = {
		"content": text_content or "",
		"embeds": embed_content or [],
		"allowed_mentions": AllowedMentions(users=True),
	}

	if file_path:
		if not os.path.exists(file_path):
			log(f"Arquivo '{file_path}' não existe! Ignorado", level="error")
		else:
			file = File(file_path, filename=os.path.basename(file_path))
			params["file"] = file
			# Código legado, migrei por abundância de caução; acho que
			# a gente nunca nem usa PNG, mas deixo abaixo como referência
			# if file_path.endswith(".png"):
			# 	embed = Embed()
			# 	embed.set_image(url=f"attachment://{file_path}")
			# 	params["embeds"].append(embed)

	async with aiohttp.ClientSession() as session:
		webhook = Webhook.from_url(webhook_url, session=session)
		try:
			await webhook.send(**params)
		except RuntimeError as e:
			log("Erro ao enviar mensagem para webhook do Discord!", level="error")
			raise e


def send_discord_embed(
	contents: List[Embed],
	slug: Literal["dbt-runs", "data-ingestion", "warning", "hci_status"],
):
	"""
	Envia um ou mais Embeds pré-formatados para o Discord

	Args:
		contents(list[Embed]): Conteúdo a ser enviado
		slug(str): Referência ao canal de destino
	"""
	asyncio.run(send_discord_webhook(slug=slug, embed_content=contents))


def send_discord_message(
	title: str,
	message: str,
	slug: Literal["dbt-runs", "data-ingestion", "warning", "hci_status"],
	file_path: str = None,
):
	"""
	Envia mensagem textual a um canal do Discord, prefixada de informações
	sobre o flow e task que fizeram a requisição

	Args:
		title(str): Título da mensagem, formatado como H2
		message(str): Conteúdo textual da mensagem
		slug(str): Referência ao canal de destino
	"""
	environment = get_current_environment()
	prefect_url = get_prefect_url()
	header_lines = [f"## {title}", f"> Environment: {environment}"]

	fr_ctx = FlowRunContext.get()
	if fr_ctx:
		flow_name = fr_ctx.flow.name
		flow_run_id = str(fr_ctx.flow_run.id)
		if prefect_url == "localhost":
			header_lines.append(f"> Flow (v3): {flow_name}")
		else:
			header_lines.append(
				f"> Flow (v3): [{flow_name}]({prefect_url}/runs/flow-run/{flow_run_id})"
			)

	tr_ctx = TaskRunContext.get()
	if tr_ctx:
		task_name = tr_ctx.task.name
		task_run_id = str(tr_ctx.task_run.id)
		if prefect_url == "localhost":
			header_lines.append(f"> Task: {task_name}")
		else:
			header_lines.append(
				f"> Task: [{task_name}]({prefect_url}/runs/task-run/{task_run_id})"
			)

	header_content = "\n".join([*header_lines, ""])
	DISCORD_MAX_CHARS = 2000
	message_max_char_count = DISCORD_MAX_CHARS - len(header_content)

	if len(message) > message_max_char_count:
		log("Mensagem excede limite de caracteres; texto será truncado", level="warning")
		message = f"{message[: message_max_char_count - 3]}..."

	asyncio.run(
		send_discord_webhook(
			slug=slug, text_content=header_content + message, file_path=file_path
		)
	)
