# -*- coding: utf-8 -*-
from typing import Literal, Optional, TypedDict
from datetime import timedelta, datetime

from prefect.schedules import Interval


def restrict_int_interval(value, min: int, max: int, default: int = 0):
	"""
	Garante que um dado valor estará dentro de um intervalo esperado; caso
	contrário, retorna `default`.
	"""
	try:
		value = int(value)
	except (ValueError, TypeError):
		return default

	if value < min: return default
	if value > max: return default
	return value


class ScheduleConfig(TypedDict):
	month: Optional[int]
	weekday: Literal[
		"monday", "tuesday", "wednesday",
		"thursday", "friday", "saturday", "sunday",
		"segunda", "terça", "quarta",
		"quinta", "sexta", "sábado", "domingo",
	]
	day: Optional[int]
	hour: Optional[int]
	minute: Optional[int]


def create_schedule(
	interval: Literal[
		"hourly", "12-hours",
		"daily", "weekly",
		"monthly", "semiannual"
	] = "daily",
	config: ScheduleConfig = None
):
	"""
	Cria schedule para um flow com o intervalo requisitado.

	Args:
		interval(str):
			Frequência a grosso modo do schedule. Espera um de alguns valores
			pré-definidos:
			* "hourly": flow executa a cada hora;
			* "12-hours": flow executa a cada 12 horas;
			* "daily": flow executa todos os dias, 1x por dia;
			* "weekly": flow executa 1x por semana;
			* "monthly": flow executa 1x por mês;
			* "semiannual": flow executa 1x a cada 6 meses
		config(dict?):
			Objeto especificando em maior granularidade a frequência.
			Por exemplo, para um flow mensal (`interval="monthly"`): \
			`config={ "day": 10, "hour": 12, "minute": 50 }`.
			Excepcionalmente para `interval="weekly"`, é possível requisitar
			um dia específico da semana: `config={ "weekday": "monday", "hour": 10 }`.
	"""
	if config is not None:
		month = restrict_int_interval(config.get("month"), 1, 12, default=1)
		weekday = (
			"monday"
			if str(config.get("weekday", "")).lower().strip() not in (
				"monday", "tuesday", "wednesday",
				"thursday", "friday", "saturday", "sunday",
				"segunda", "terça", "quarta",
				"quinta", "sexta", "sábado", "domingo",
			)
			else str(config.get("weekday")).lower().strip()
		)
		day = restrict_int_interval(config.get("day"), 1, 28, default=1)
		hour = restrict_int_interval(config.get("hour"), 0, 23)
		minute = restrict_int_interval(config.get("minute"), 0, 59)

	if interval == "hourly":
		return Interval(
			timedelta(hours=1),
			anchor_date=datetime(2026, 1, 1, 0, minute),
			timezone="America/Sao_Paulo"
		)

	if interval == "12-hours":
		return Interval(
			timedelta(hours=12),
			anchor_date=datetime(2026, 1, 1, hour, minute),
			timezone="America/Sao_Paulo"
		)

	if interval == "daily":
		return Interval(
			timedelta(days=1),
			anchor_date=datetime(2026, 1, 1, hour, minute),
			timezone="America/Sao_Paulo"
		)

	if interval == "weekly":
		# 5 jan 2026 foi segunda-feira
		day_offset = 0
		if weekday in ("monday", "segunda"):     day_offset = 0
		elif weekday in ("tuesday", "terça"):    day_offset = 1
		elif weekday in ("wednesday", "quarta"): day_offset = 2
		elif weekday in ("thursday", "quinta"):  day_offset = 3
		elif weekday in ("friday", "sexta"):     day_offset = 4
		elif weekday in ("saturday", "sábado"):  day_offset = 5
		elif weekday in ("sunday", "domingo"):   day_offset = 6
		return Interval(
			timedelta(days=7),
			anchor_date=datetime(2026, 1, 5+day_offset, hour, minute),
			timezone="America/Sao_Paulo"
		)

	if interval == "monthly":
		return Interval(
			timedelta(days=30),
			anchor_date=datetime(2026, 1, day, hour, minute),
			timezone="America/Sao_Paulo"
		)

	if interval == "semiannual":
		return Interval(
			timedelta(days=6*30),
			anchor_date=datetime(2026, month, day, hour, minute),
			timezone="America/Sao_Paulo"
		)

	raise ValueError(f"Valor para intervalo não reconhecido: '{interval}'")
