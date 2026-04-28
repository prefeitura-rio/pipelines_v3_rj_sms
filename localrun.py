# -*- coding: utf-8 -*-
import importlib
import yaml
from prefect.testing.utilities import prefect_test_harness


def get_default_case_config(case_slug: str) -> dict:
  case_file = yaml.safe_load(open("localrun.cases.yaml", encoding="utf-8"))

  for case in case_file["cases"]:
    if case["case_slug"] == case_slug:
      return case

  raise ValueError(f"Flow '{case_slug}' não encontrado em `localrun.cases.yaml`!")


if __name__ == "__main__":
  try:
    selected_case = yaml.safe_load(open("localrun.selected.yaml"))
  except FileNotFoundError:
    raise FileNotFoundError("Por favor, crie um arquivo `localrun.selected.yaml`.")

  # Verifica se o flow existe em `localrun.cases.yaml`
  default_case = get_default_case_config(selected_case.get("selected_case"))

  # Importa flow e obtém objeto executável
  flow_name = default_case.get("flow_name")
  flow_module = importlib.import_module(default_case.get("flow_path"))
  flow = getattr(flow_module, flow_name)

  override_params = selected_case.get("override_params", {})
  default_params = default_case.get("params")
  flow_params = {**default_params, **override_params}

  # Executa o flow localmente
  # Esse passo abre um servidor uvicorn em uma porta semi aleatória
  # (limite de tempo para o início do servidor aqui é 60s)
  # O servidor é configurado como "efêmero", e é apagado logo depois do flow executar
  with prefect_test_harness(server_startup_timeout=60):
    flow(**flow_params)
