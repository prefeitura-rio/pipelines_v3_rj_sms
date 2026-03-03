# -*- coding: utf-8 -*-
from os import getenv

from .logger import log


def getenv_or_action(key: str, *, action: str = "raise", default: str = None):
	"""
	Returns the value of the environment variable with the given key, or the result of the
	given action if the environment variable is not set.

	Args:
		key (str): The name of the environment variable.
		action (str, optional): The name of the action to perform if neither the environment
			variable nor a default value is set. Valid actions are "raise", "warn" and "ignore".
			Defaults to "raise".
		default (str, optional): The default value to return if the environment variable is
			not set.

	Raises:
		ValueError: If the action is not valid.

	Returns:
		str: The value of the environment variable, or the result of the action.
	"""
	if action not in ["raise", "warn", "ignore"]:
		raise ValueError(f"Invalid action: {action}")

	value = getenv(key, default)

	if value is None:
		if action == "raise":
			raise ValueError(f"Environment variable {key} is not set")
		elif action == "warn":
			log(f"WARNING: Environment variable {key} is not set", level="warning")
		elif action == "ignore":
			pass

	return value

