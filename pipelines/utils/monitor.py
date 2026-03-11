# -*- coding: utf-8 -*-
from typing import List, Literal

import aiohttp
from discord import AllowedMentions, Embed, Webhook

from pipelines.utils.env import get_current_environment
from pipelines.utils.infisical import get_secret


async def send_discord_embed(
	contents: List[Embed],
	monitor_slug: str = Literal[
		"endpoint-health", "dbt-runs", "data-ingestion", "warning", "hci_status"
	],
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
	webhook_url = get_secret(secret_name=secret_name, environment=environment).get(secret_name)

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
