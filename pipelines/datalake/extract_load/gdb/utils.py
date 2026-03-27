# -*- coding: utf-8 -*-
import re

from pipelines.utils.datetime import now
from pipelines.utils.google import dissect_gcs_uri


def format_reference_date(refdate: str | None, gcs_uri: str) -> str:
	# Se não há data especificada
	if refdate is None or not refdate:
		# Tentamos pegar do nome do arquivo
		# Os arquivos normalmente terminam ou com MMYYYY ou YYYYMM
		uri = dissect_gcs_uri(gcs_uri)
		filename = uri["filename_no_ext"]

		# Se o arquivo termina com 6 dígitos
		if re.search(r"[0-9]{6}$", filename):
			# Pegamos somente os dígitos
			dt = filename[-6:]
			# Se os dois primeiros são > 12, então a data começa com ano
			if int(dt[:2]) > 12:
				# [YYYY]MM
				year = dt[:4]
				# YYYY[MM]
				month = dt[4:]
			# Senão, começa com <= 12, e não estamos em um ano <1200
			# Então o mês vem primeiro
			else:
				# [MM]YYYY
				month = dt[:2]
				# MM[YYYY]
				year = dt[2:]

			# Filtros finais de data realista
			if 2000 < int(year) < 2100 and 0 < int(month) < 13:
				return f"{year}-{month}-01"
		# Aqui, o arquivo não termina com mês e ano; não temos
		# ideia de qual data ele se refere, e não recebemos
		# `refdate` como parâmetro. Então a melhor opção é a
		# data atual :/
		dt = now()
		return dt.strftime("%Y-%m-01")

	# Aqui, recebemos data de referência! Uhules
	refdate = str(refdate).strip()
	# YYYY-MM?-DD?
	if re.search(r"^[0-9]{4}$", refdate):
		return f"{refdate}-01-01"
	if re.search(r"^[0-9]{4}[\-\/][0-9]{2}$", refdate):
		return f"{refdate}-01".replace("/", "-")
	if re.search(r"^[0-9]{4}[\-\/][0-9]{2}[\-\/][0-9]{2}$", refdate):
		return f"{refdate}".replace("/", "-")
	raise ValueError(
		f"Formato de data referência é 'YYYY(-MM(-DD)?)?'; "
		f"valor recebido, '{refdate}', não encaixa"
	)
