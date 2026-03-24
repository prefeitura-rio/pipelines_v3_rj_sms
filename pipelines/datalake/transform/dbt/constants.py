# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
	REPOSITORY_URL = "https://github.com/prefeitura-rio/queries-rj-sms.git"

	GCS_BUCKET = {"prod": "rj-sms_dbt", "dev": "rj-sms-dev_dbt"}
