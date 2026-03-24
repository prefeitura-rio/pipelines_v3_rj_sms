# -*- coding: utf-8 -*-
import asyncio
from typing import List, Literal

import aiohttp
from discord import AllowedMentions, Embed, File, Webhook
from prefect.context import FlowRunContext, TaskRunContext

from pipelines.utils.env import get_current_environment, get_prefect_url
from pipelines.utils.infisical import get_secret


## TODO: refatorar pra ser uma função só


async def send_discord_webhook(
	text_content: str,
	file_path: str = None,
	username: str = None,
	suppress_embeds: bool = False,
	monitor_slug: Literal[
		"endpoint-health", "dbt-runs", "data-ingestion", "warning", "hci_status"
	] = None,
):
	"""
	Envia mensagem para um webhook do Discord

	Args:
		message (str): The message to send.
		username (str, optional): The username to use when sending the message. Defaults to None.
	"""
	environment = get_current_environment()
	secret_name = f"DISCORD_WEBHOOK_URL_{monitor_slug.upper()}"
	webhook_url = get_secret(secret_name=secret_name, environment=environment).get(
		secret_name
	)

	if len(text_content) > 2000:
		raise ValueError(
			f"Message content is too long: {len(text_content)} > 2000 characters."
		)

	async with aiohttp.ClientSession() as session:
		kwargs = {
			"content": text_content,
			"allowed_mentions": AllowedMentions(users=True),
		}
		if suppress_embeds:
			kwargs["suppress_embeds"] = True
		if username:
			kwargs["username"] = username
		if file_path:
			file = File(file_path, filename=file_path)

			if ".png" in file_path:
				embed = Embed()
				embed.set_image(url=f"attachment://{file_path}")
				kwargs["embed"] = embed

			kwargs["file"] = file

		webhook = Webhook.from_url(webhook_url, session=session)
		try:
			await webhook.send(**kwargs)
		except RuntimeError:
			raise ValueError(f"Error sending message to Discord webhook: {webhook_url}")


async def send_discord_embed(
	contents: List[Embed],
	monitor_slug: Literal[
		"endpoint-health", "dbt-runs", "data-ingestion", "warning", "hci_status"
	] = None,
	username: str = None,
):
	"""
	Sends embedded content to a Discord webhook

	Args:
		content (Embed): The content to send.
		monitor_slug (str): The channel to send it to.
		username (str, optional): The username to use when sending the message. Defaults to None.
	"""
	environment = get_current_environment()
	secret_name = f"DISCORD_WEBHOOK_URL_{monitor_slug.upper()}"
	webhook_url = get_secret(secret_name=secret_name, environment=environment).get(
		secret_name
	)

	async with aiohttp.ClientSession() as session:
		kwargs = {
			"content": "",
			"embeds": contents,
			"allowed_mentions": AllowedMentions(users=True),
		}
		if username:
			kwargs["username"] = username

		webhook = Webhook.from_url(webhook_url, session=session)
		try:
			await webhook.send(**kwargs)
		except RuntimeError:
			raise ValueError(f"Error sending message to Discord webhook: {webhook_url}")


def send_discord_message(
	title: str,
	message: str,
	monitor_slug: str,
	file_path: str = None,
	username: str = None,
	suppress_embeds: bool = False,
):
	"""
	Envia mensagem para um canal do Discord

	Args:
			title (str): The title of the message.
			message (str): The content of the message.
			username (str, optional): The username to be used for the webhook. Defaults to None.
	"""
	fr_ctx = FlowRunContext.get()
	tr_ctx = TaskRunContext.get()

	environment = get_current_environment()
	flow_name = fr_ctx.flow.name
	flow_run_id = str(fr_ctx.flow_run.id)
	task_name = tr_ctx.task.name
	task_run_id = str(tr_ctx.task_run.id)

	prefect_url = get_prefect_url()
	header_content = f"""
## {title}
> Environment: {environment}
> Flow Run: [{flow_name}]({prefect_url}/runs/flow-run/{flow_run_id})
> Task Run: [{task_name}]({prefect_url}/runs/task-run/{task_run_id})
	"""
	# Calculate max char count for message
	message_max_char_count = 2000 - len(header_content)
	line_max_char_count = 5000

	# Split message into lines
	message_lines = message.split("\n")

	# Split message into pages
	pages = []
	current_page = ""
	for line in message_lines:
		if len(current_page) + 2 + len(line) < message_max_char_count:
			current_page += "\n" + line
		else:
			pages.append(current_page)
			if len(line) <= message_max_char_count:
				current_page = line
			# Handle lines that are too long
			elif len(line) > message_max_char_count and len(line) <= line_max_char_count:
				for i in range(0, len(line), message_max_char_count):
					chunk = line[i : i + message_max_char_count]
					if i == 0:
						current_page = chunk
					else:
						pages.append(current_page)
						current_page = chunk
			else:
				raise ValueError(
					f"Line length exceeds maximum allowed characters: {len(line)} > {message_max_char_count}"
				)

	# Append last page
	pages.append(current_page)

	# Build message content using Header in first page
	message_contents = []
	for page_idx, page in enumerate(pages):
		if page_idx == 0:
			message_contents.append(header_content + page)
		else:
			message_contents.append(page)

	# Send message to Discord
	async def async_send(contents):
		for content in contents:
			await send_discord_webhook(
				text_content=content,
				file_path=file_path,
				username=username,
				monitor_slug=monitor_slug,
				suppress_embeds=suppress_embeds,
			)

	asyncio.run(async_send(message_contents))
