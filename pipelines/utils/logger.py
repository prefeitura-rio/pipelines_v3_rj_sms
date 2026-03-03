# -*- coding: utf-8 -*-
import logging

from prefect.logging import get_run_logger
from typing_extensions import Literal


LEVELS_CONFIG = {
	"debug": {"type": logging.DEBUG, "discord_forwarding": False, "icon": "🟦"},
	"info": {"type": logging.INFO, "discord_forwarding": False, "icon": "🟩"},
	"warning": {"type": logging.WARNING, "discord_forwarding": False, "icon": "⚠️"},
	"error": {"type": logging.ERROR, "discord_forwarding": True, "icon": "❌"},
	"critical": {"type": logging.CRITICAL, "discord_forwarding": True, "icon": "🔴"},
}

def log(
	msg: str,
	level: Literal["debug", "info", "warning", "error", "critical"] = "info",
) -> None:
	"""
	Args:
		msg (str):
			Mensagem a ser logada.
		level (Literal["debug", "info", "warning", "error", "critical"]):
			Nível de gravidade da mensagem.
	Returns:
		None
	"""
	get_run_logger().log(LEVELS_CONFIG[level]["type"], msg)

	# TODO: discord

