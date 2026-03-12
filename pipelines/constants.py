# -*- coding: utf-8 -*-
import pytz
from enum import Enum


class constants(Enum):

	"""
	Valores permitidos para a variável `environment`
	"""
	ALLOWED_ENVIRONMENTS = [
		"dev", "staging", "prod"
	]

	"""
	Mapeamento de environment para nome de projeto
	"""
	GOOGLE_CLOUD_PROJECT = {
		"dev": "rj-sms-dev",
		"staging": "rj-sms-dev",
		"prod": "rj-sms"
	}

	TIMEZONE = "America/Sao_Paulo"
	PYTZ_TIMEZONE = pytz.timezone(TIMEZONE)


	################################
	##      DISCORD MENTIONS      ##
	################################

	#############
	##  S/CIT  ##
	#############
	CIT_ID = "&1224334248345862164"
	# Equipe de dados
	PEDRO_ID   =  "210481264145334273"  # Pedro Marques
	DANIEL_ID  = "1153123302508859422"  # Daniel Lira
	HERIAN_ID  =  "213846751247859712"  # Herian Cavalcante
	KAREN_ID   =  "722874135893508127"  # Karen Pacheco
	AVELLAR_ID =   "95182393446502400"  # Matheus Avellar
	# Produto
	NATACHA_ID = "1121558659768528916"  # Natacha Pragana
	POLIANA_ID = "1315728001320620064"  # Poliana Lucena
	# Misc
	DANILO_ID  = "1147152438487416873"  # Danilo Fonseca

	#############################
	##  Outras subsecretarias  ##
	#############################
	DAYANE_ID  =  "316705041161388032"  # Dayane Ramos
	MATHEUS_ID = "1184846547242995722"  # Matheus Miloski

