# -*- coding: utf-8 -*-
import logging

from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger
from typing_extensions import Literal


LEVELS_CONFIG = {
	"debug":    { "type": logging.DEBUG,    "icon": "🟦" },
	"info":     { "type": logging.INFO,     "icon": "🟩" },
	"warning":  { "type": logging.WARNING,  "icon": "⚠️" },
	"error":    { "type": logging.ERROR,    "icon": "❌" },
	"critical": { "type": logging.CRITICAL, "icon": "🔴" },
}  # fmt: skip


def log(
  *args,
  level: Literal["debug", "info", "warning", "error", "critical"] = "info",
  fwd_discord: bool = False,
) -> None:
  """
  Args:
          *args (...):
                  Mensagem a ser logada.
          level (Literal["debug", "info", "warning", "error", "critical"]):
                  Nível de gravidade da mensagem.
          fwd_discord (bool?):
                  Flag de conveniência; quando `True`, encaminha a mensagem
                  também para o Discord.
  """
  try:
    get_run_logger().log(LEVELS_CONFIG[level]["type"], *args)
  except MissingContextError:
    # Não há flow executando (ainda, ou mais)
    # A função provavelmente foi chamada p.ex. em evento lifecycle
    print(*args)

  if fwd_discord:
    icon = LEVELS_CONFIG[level]["icon"]
    title = f"{icon} Log {level.capitalize()}"
    msg = "".join(args)
    slug = "error" if level in ["error", "critical"] else "warning"

    # Precisamos importar aqui porque `send_discord_message()`
    # usa `log()` = dependência circular. Importando dentro
    # da função em si, a dependência só é calculada quando
    # (e se) alguém chamar com `fwd_discord=True`
    from pipelines.utils.monitor import send_discord_message

    send_discord_message(title=title, message=msg, slug=slug)
