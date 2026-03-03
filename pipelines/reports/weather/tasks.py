# -*- coding: utf-8 -*-
import httpx

from prefect import task
from pipelines.utils.logger import log

from .constants import (
	BASE_API_URL,
	FORECAST_ENDPOINT
)


@task(retries=3, timeout_seconds=15)
def fetch_weather(lat: float, lon: float, environment: str="dev") -> dict:
	log(f"Requisitando previsão do tempo para ('{lat}', '{lon}')...", level="info")
	data = httpx.get(
		f"{BASE_API_URL}{FORECAST_ENDPOINT}",
		params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
	)
	log("Previsão recebida!", level="info")
	return data.json()


@task()
def print_report(data: dict) -> None:
	temperature_unit = data["hourly_units"]["temperature_2m"]

	times = data["hourly"]["time"][:24]
	temperatures = data["hourly"]["temperature_2m"][:24]

	output = [
		"",  # quebra de linha no início, formata melhor
		"Data/hora        | Temperatura",
		"------------------------------"
	]

	for time, temp in zip(times, temperatures):
		output.append(
			f"{time} - {temp} {temperature_unit}"
		)

	log("\n".join(output), level="info")
