# -*- coding: utf-8 -*-
import datetime
import re
from typing import Optional
from zoneinfo import ZoneInfo
from dateutil import parser

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
    log("The input dated is not a relative date, converting to datetime", level="info")
    result = datetime.datetime.fromisoformat(relative_date)

  log(f"Relative date is {relative_date}, returning {result}", level="info")
  return result


def parse_date_or_today(
  date: Optional[str], subtract_days_from_today: Optional[int] = None
) -> datetime.datetime:
  """
  Recebe string de data (ex.: "2025-07-18", "30/10/1999")
  e retorna objeto `datetime` a partir dela. Se receber
  string vazia ou `None`, retorna `datetime.now`.

  Os formatos esperados são YYYY-MM-DD e DD/MM/YYYY. A
  função tenta fazer parsing de outros formatos via
  `parser.parse(date, ignoretz=True, dayfirst=True)` mas,
  se possível, use um dos formatos esperados.

  Exemplos de uso, se hoje é 18/jul/2025:
  ```
  >>> parse_date_or_today("")
  datetime.datetime(2025, 7, 18, 0, 0)
  >>> parse_date_or_today(None)
  datetime.datetime(2025, 7, 18, 0, 0)
  >>> parse_date_or_today("2012-01-05")
  datetime.datetime(2012, 1, 5, 0, 0)
  >>> parse_date_or_today("05/01/2012")
  datetime.datetime(2012, 1, 5, 0, 0)
  >>> parse_date_or_today("05-01-2012")
  datetime.datetime(2012, 1, 5, 0, 0)
  ```
  """
  # Se não recebeu data nenhuma
  if date is None or len(date) <= 0:
    # Retorna a data atual em formato igual ao retornado
    # pelas funções de parsing
    today = now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    if subtract_days_from_today:
      return today - datetime.timedelta(days=subtract_days_from_today)
    return today

  # Caso contrário, vamos tentar interpretar a string recebida
  date = date.strip()
  # Se recebemos o formato ISO 'YYYY-MM-DD'
  if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", date):
    return datetime.datetime.fromisoformat(date)
  # Se recebemos o formato 'DD/MM/YYYY'
  if re.fullmatch(r"[0-9]{2}/[0-9]{2}/[0-9]{4}", date):
    return datetime.datetime.strptime(date, "%d/%m/%Y")
  # Senão, faz última tentativa de parsing da string, pode dar erro
  return parser.parse(date, ignoretz=True, dayfirst=True)


def get_age_from_birthdate(birthdate: str, today: str = None) -> int | None:
  """
  Args:
    birthdate(str):
      Data no formato ISO: "YYYY-MM-DD".
    today(str?):
      Referência para o que é 'hoje', no formato ISO: "YYYY-MM-DD".
      Se omitido, `datetime.now` é usado.

  Returns:
    age(int):
      Idade de pessoa nascida na data especificada, se possível de
      calcular; caso contrário, `None`.

  Exemplos de uso, se hoje é 08/oct/2025:
  >>> get_age_from_birthdate("2024-10-09")  # Antes de 1 ano completo
  0
  >>> get_age_from_birthdate("2024-10-08")  # Exatamente 1 ano
  1
  >>> get_age_from_birthdate("2000-12-31")
  24
  >>> get_age_from_birthdate("2000-05-01")
  25
  >>> get_age_from_birthdate("1900-01-01")
  125
  >>> get_age_from_birthdate("1900-01-01", today="1900-12-31")
  0
  >>> get_age_from_birthdate("1900-01-01", today="1901-01-01")
  1
  >>> get_age_from_birthdate(None)
  None
  >>> get_age_from_birthdate("")
  None
  >>> get_age_from_birthdate("banana")
  [...] WARNING - prefect | Formato esperado é `YYYY-MM-DD`; recebido 'banana'
  >>> get_age_from_birthdate("2000-01-01", today="banana")
  [...] WARNING - prefect | Formato esperado é `YYYY-MM-DD`; recebido 'banana'
  """
  if birthdate is None:
    return None

  birthdate = str(birthdate).strip()
  if len(birthdate) <= 0:
    return None

  if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", birthdate):
    log(f"Formato esperado é `YYYY-MM-DD`; recebido {repr(birthdate)}", level="warning")
    return None

  if today is not None and not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", today):
    log(f"Formato esperado é `YYYY-MM-DD`; recebido {repr(today)}", level="warning")
    return None

  dt_today = datetime.datetime.fromisoformat(today) if today is not None else now()
  dt_born = datetime.datetime.fromisoformat(birthdate)

  # [Ref] https://stackoverflow.com/a/9754466/4824627
  return (
    dt_today.year
    - dt_born.year
    - int((dt_today.month, dt_today.day) < (dt_born.month, dt_born.day))
  )
