# -*- coding: utf-8 -*-
from prefect.docker import DockerImage
from prefect.flows import Flow

import pipelines.flows as flows


def deploy_flow(flow_name: str):
	print(f"Procurando flow de nome '{flow_name}'...")

	# FIXME: verificar riscos
	flow = getattr(flows, flow_name)
	if not flow:
		raise ValueError(f"Flow de nome '{flow_name}' não encontrado! Ele foi adicionado a `pipelines/flows.py`?")

	print(f"Flow de nome '{flow_name}' encontrado! Fazendo deploy...")

	flow: Flow
	flow.deploy(
		name=flow.name,
		work_pool_name="gcp-wp",  # FIXME: não gosto que seja hardcoded assim
		image=DockerImage(
			name=f"southamerica-east1-docker.pkg.dev/rj-sms/pipelines-v3-rj-sms/{flow_name}",
			tag="latest"
		),
		#schedules= (TODO: 1. conferir se é dev; 2. pensar em como passar os schedules pra cá)
	)

if __name__ == "__main__":
	deploy_flow("weather_report")  # FIXME
