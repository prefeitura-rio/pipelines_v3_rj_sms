# -*- coding: utf-8 -*-
from enum import Enum
from pipelines.constants import constants as global_consts


class constants(Enum):
	GCS_BUCKET = {"prod": "rj-sms_dbt", "dev": "rj-sms-dev_dbt"}

	# Chaves equivalem ao $DBT_USER de cada desenvolvedor,
	# com todas as letras em minúsculas
	OWNERS = {
		## CIT
		"cit": global_consts.CIT_ID.value,
		"avellar": global_consts.AVELLAR_ID.value,
		"herian": global_consts.HERIAN_ID.value,
		"daniellira": global_consts.DANIEL_ID.value,
		"karen": global_consts.KAREN_ID.value,
		"pedro": global_consts.PEDRO_ID.value,
		## Outros
		"dayaners": global_consts.DAYANE_ID.value,
		"miloskimatheus": global_consts.MILOSKI_ID.value,
	}
