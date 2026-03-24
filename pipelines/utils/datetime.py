# -*- coding: utf-8 -*-
import datetime
from zoneinfo import ZoneInfo


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
