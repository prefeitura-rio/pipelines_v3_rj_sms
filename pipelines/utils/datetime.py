# -*- coding: utf-8 -*-
import datetime
from typing import Optional
from zoneinfo import ZoneInfo

from pipelines.utils.logger import log


UTC_TZ = ZoneInfo("UTC")
SAO_PAULO_TZ = ZoneInfo("America/Sao_Paulo")


def now(utc: bool = False) -> datetime.datetime:
	"""
	Retorna datetime.now() ou em BRT (padrão) ou em UTC

	Args:
		utc(bool?):
			Se deve usar fuso horário UTC. Por padrão, é `False`,
			e usa o fuso BRT (America/Sao_Paulo).
	"""
	if utc:
		return datetime.datetime.now(tz=UTC_TZ)
	return datetime.datetime.now(tz=SAO_PAULO_TZ)


def today_str() -> str:
	"""Retorna o dia atual (fuso BRT) como 'YYYY-MM-DD'"""
	return now().date().isoformat()

def now_str() -> str:
	"""Retorna data/hora atual (fuso BRT) como 'YYYY-MM-DD HH:MM:SS'"""
	return now().strftime("%Y-%m-%d %H:%M:%S")

def current_year() -> int:
	return now().year


def from_relative_date(
	relative_date: Optional[str] = None,
) -> Optional[datetime.date | datetime.datetime]:
	"""
	Converte uma data relativa para um objeto de data.

	Suporta os formatos:
		`D-N`: data atual menos `N` dias
		`M-N`: primeiro dia do mês atual menos `N` meses
		`Y-N`: primeiro dia do ano atual menos `N` anos

	Caso o valor não seja uma data relativa, tenta convertê-lo
	para `datetime` via `datetime.fromisoformat()`.
	"""
	if relative_date is None:
		log("Relative date is None, returning None", level="info")
		return None

	current_datetime = now()
	current_date = current_datetime.date()

	if relative_date.startswith(("D-", "M-", "Y-")):
		quantity = int(relative_date.split("-", maxsplit=1)[1])

		if relative_date.startswith("D-"):
			result = current_date - datetime.timedelta(days=quantity)
		elif relative_date.startswith("M-"):
			month_index = current_date.month - quantity - 1
			year = current_date.year + (month_index // 12)
			month = (month_index % 12) + 1
			result = datetime.date(year, month, 1)
		else:
			result = datetime.date(current_date.year - quantity, 1, 1)
	else:
		log(
			"The input dated is not a relative date, converting to datetime", level="info"
		)
		result = datetime.datetime.fromisoformat(relative_date)

	log(f"Relative date is {relative_date}, returning {result}", level="info")
	return result
