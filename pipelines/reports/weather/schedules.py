# -*- coding: utf-8 -*-
# from pipelines.utils.schedules import create_schedule


schedules = [
  ## Exemplo de hora em hora, no minuto 30 (9:30, 10:30, ...):
  # create_schedule(
  # 	parameters={"environment": "dev", "lat": -22.91122, "lon": -43.20562},
  # 	interval="hourly",
  # 	config={"minute": 30},
  # ),
  ## Exemplo semanal:
  # create_schedule(
  # 	interval="weekly",
  # 	config={
  # 		"weekday": "monday",
  # 		"hour": 10,
  # 		"minute": 50
  # 	}
  # ),
  ## Exemplo customizado, depois de importar
  ## `from datetime import timedelta, datetime`
  ## e `from prefect.schedules import Interval`:
  # Interval(
  # 	# A cada 3 dias
  # 	timedelta(days=3),
  # 	# ...contados a partir do dia 1/jan, roda às 10:50
  # 	anchor_date=datetime(2026, 1, 1, 10, 50, tzinfo=constants.TIMEZONE.value),
  # 	timezone=constants.TIMEZONE_NAME.value,
  # )
]
