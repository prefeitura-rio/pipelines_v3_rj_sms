# -*- coding: utf-8 -*-
from typing import List, Literal, Optional, TypedDict
from datetime import timedelta, datetime

from prefect.schedules import Interval

from pipelines.constants import constants


def restrict_int_interval(value, min: int, max: int, default: int = 0):
	"""
	Garante que um dado valor estará dentro de um intervalo esperado; caso
	contrário, retorna `default`.
	"""
	try:
		value = int(value)
	except (ValueError, TypeError):
		return default

	if value < min:
		return default
	if value > max:
		return default
	return value


class ScheduleConfig(TypedDict):
	month: Optional[int]
	weekday: Literal[
		"monday",    "segunda",
		"tuesday",   "terça",
		"wednesday", "quarta",
		"thursday",  "quinta",
		"friday",    "sexta",
		"saturday",  "sábado",
		"sunday",    "domingo",
	]  # fmt: skip
	day: Optional[int]
	hour: Optional[int]
	minute: Optional[int]


def create_schedule_list(
	parameters_list: List[dict],
	interval: Literal[
		"hourly", "12-hours", "daily", "weekly", "monthly", "semiannual"
	] = "daily",
	config: ScheduleConfig = None,
):
	return [
		create_schedule(parameters=params, interval=interval, config=config)
		for params in parameters_list
	]


def create_schedule(
	parameters: dict,
	interval: Literal[
		"hourly", "12-hours", "daily", "weekly", "monthly", "semiannual"
	] = "daily",
	config: ScheduleConfig = None,
):
	"""
	Cria schedule para um flow com o intervalo requisitado.

	Args:
		parameters(dict):
			Parâmetros a serem passados para o flow nesse schedule.
		interval(str):
			Frequência a grosso modo do schedule. Espera um de alguns valores
			pré-definidos: \
			* `"hourly"`: flow executa a cada hora;
			* `"12-hours"`: flow executa a cada 12 horas;
			* `"daily"`: flow executa todos os dias, 1x por dia;
			* `"weekly"`: flow executa 1x por semana;
			* `"monthly"`: flow executa 1x por mês;
			* `"semiannual"`: flow executa 1x a cada 6 meses
		config(dict?):
			Objeto especificando em maior granularidade a frequência.
			Propriedades utilizáveis são:\
			* `"minute"` int([0-59]) —
				Minuto em que o flow executa
			* `"hour"` int([0-23]) —
				Hora em que o flow executa
			* `"day"`: int([1-28]) —
				Dia do mês em que o flow executa, limitado ao dia 28; só usado em flows mensais
			* `"weekday"`: str({ "monday", "segunda", "tuesday", "terça", ... }) —
				Dia da semana em que o flow executa; só usado com `interval="weekly"`
			* `"month"`: int([1-12]) —
				Mês da primeira execução do flow (a segunda será mês + 6); só usado com `interval="semiannual"`

	Exemplos:
	```python
	# Uma vez por hora, no minuto 30 (i.e. 9:30, 10:30, ...)
	create_schedule(
		parameters={ ... },
		interval="hourly",
		config={ "minute": 30 },
	)
	# Todo dia 5 do mês, às 14:21
	create_schedule(
		parameters={ ... },
		interval="monthly",
		config={ "day": 5, "hour": 14, "minute": 21 }
	)
	# Toda terça às 9:50
	create_schedule(
		parameters={ ... },
		interval="weekly",
		config={ "weekday": "tuesday", "hour": 9, "minute": 50 }
		# ou "weekday": "terça"
	)
	```
	"""
	month = 1
	weekday = "monday"
	day = 1
	hour = 0
	minute = 0

	if config is not None:
		month = restrict_int_interval(config.get("month"), 1, 12, default=1)
		_processed_weekday = str(config.get("weekday", "")).lower().strip()
		weekday = (
			"monday"
			if _processed_weekday not in (
				"tuesday",   "terça",
				"wednesday", "quarta",
				"thursday",  "quinta",
				"friday",    "sexta",
				"saturday",  "sábado",
				"sunday",    "domingo",
			)
			else _processed_weekday
		)  # fmt: skip
		day = restrict_int_interval(config.get("day"), 1, 28, default=1)
		hour = restrict_int_interval(config.get("hour"), 0, 23)
		minute = restrict_int_interval(config.get("minute"), 0, 59)

	if interval == "hourly":
		return Interval(
			timedelta(hours=1),
			anchor_date=datetime(2026, 1, 1, 0, minute, tzinfo=constants.TIMEZONE.value),
			timezone=constants.TIMEZONE_NAME.value,
			parameters=parameters,
		)

	if interval == "12-hours":
		return Interval(
			timedelta(hours=12),
			anchor_date=datetime(
				2026, 1, 1, hour, minute, tzinfo=constants.TIMEZONE.value
			),
			timezone=constants.TIMEZONE_NAME.value,
			parameters=parameters,
		)

	if interval == "daily":
		return Interval(
			timedelta(days=1),
			anchor_date=datetime(
				2026, 1, 1, hour, minute, tzinfo=constants.TIMEZONE.value
			),
			timezone=constants.TIMEZONE_NAME.value,
			parameters=parameters,
		)

	if interval == "weekly":
		# 5 jan 2026 foi segunda-feira
		# fmt: off
		day_offset = 0
		if   weekday in ("monday",    "segunda"): day_offset = 0
		elif weekday in ("tuesday",   "terça"):   day_offset = 1
		elif weekday in ("wednesday", "quarta"):  day_offset = 2
		elif weekday in ("thursday",  "quinta"):  day_offset = 3
		elif weekday in ("friday",    "sexta"):   day_offset = 4
		elif weekday in ("saturday",  "sábado"):  day_offset = 5
		elif weekday in ("sunday",    "domingo"): day_offset = 6
		# fmt: on
		return Interval(
			timedelta(days=7),
			anchor_date=datetime(
				2026, 1, 5 + day_offset, hour, minute, tzinfo=constants.TIMEZONE.value
			),
			timezone=constants.TIMEZONE_NAME.value,
			parameters=parameters,
		)

	if interval == "monthly":
		return Interval(
			timedelta(days=30),
			anchor_date=datetime(
				2026, 1, day, hour, minute, tzinfo=constants.TIMEZONE.value
			),
			timezone=constants.TIMEZONE_NAME.value,
			parameters=parameters,
		)

	if interval == "semiannual":
		return Interval(
			timedelta(days=6 * 30),
			anchor_date=datetime(
				2026, month, day, hour, minute, tzinfo=constants.TIMEZONE.value
			),
			timezone=constants.TIMEZONE_NAME.value,
			parameters=parameters,
		)

	raise ValueError(f"Valor para intervalo não reconhecido: '{interval}'")
