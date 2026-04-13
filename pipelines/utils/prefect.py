# -*- coding: utf-8 -*-
import asyncio
import re
from typing import Any, Callable, Literal, Union, List, Optional
import unicodedata

from prefect import Task, get_client
from prefect.context import FlowRunContext
from prefect.flows import Flow as OriginalFlow, FlowDecorator as OriginalFlowDecorator
from prefect.schedules import Schedule

from pipelines.utils.env import get_current_environment, is_dev_run
from pipelines.utils.infisical import inject_bd_credentials
from pipelines.utils.logger import log


#################
## FLOWS
#################


class Flow(OriginalFlow):
	def __init__(self, *args, owners: Optional[List[str]] = None, **kwargs):
		# Guarda a lista de owners como atributo
		self.owners = owners or []
		# Continua chamando o __init__ do Flow original
		super().__init__(*args, **kwargs)

	def get_owners(self):
		return self.owners


class FlowDecorator(OriginalFlowDecorator):
	name: str = None
	description: str = ""
	state_handlers: List[Callable] = None
	owners: List[str] = None
	log_prints: bool = False

	def __init__(
		self,
		*args,
		name: str = None,
		description: str = "",
		state_handlers: List[Callable] = None,
		owners: Optional[List[str]] = None,
		log_prints: bool = False,
		**kwargs,
	):
		self.name = name
		self.description = description or ""
		self.state_handlers = state_handlers or []
		self.owners = owners or []
		self.log_prints = log_prints

	def __call__(self, *args, **kwargs):
		return Flow(
			*args,
			name=self.name,
			description=self.description,
			owners=self.owners,
			log_prints=self.log_prints,
			on_completion=[*self.state_handlers],
			on_failure=[*self.state_handlers],
			on_cancellation=[*self.state_handlers],
			on_crashed=[*self.state_handlers],
			on_running=[*self.state_handlers],
			validate_parameters=False,
			**kwargs,
		)


flow = FlowDecorator


#################
## TASKS
#################


def authenticated_task(
	fn: Callable = None, **task_init_kwargs: Any
) -> Union[Task, Callable[[Callable], Task]]:
	"""
	Função que injeta credenciais no ambiente de uma Task.
	Se `fn` estiver definido, retorna uma instância `Task(fn=fn, **task_init_kwargs)`.
	Se `fn` for None, retorna um decorator que pode ser usado para tornar uma função
	em uma Task autenticada. Ex.:

	```python
	from pipelines.utils.prefect import authenticated_task as task

	@task(...)
	def xxxxx():
		# ...
	```
	"""

	def inject_credential_setting_in_function(function):

		def new_function(**kwargs):
			env = get_current_environment()
			log(f"[Injected] Set BD credentials for environment {env}", level="debug")
			inject_bd_credentials(environment=env)
			log("[Injected] Now executing function normally...", level="debug")
			return function(**kwargs)

		new_function.__name__ = function.__name__
		return new_function

	# Instância de Task
	if fn is not None:
		return Task(fn=inject_credential_setting_in_function(fn), **task_init_kwargs)
	# Decorator
	return lambda any_function: Task(
		fn=inject_credential_setting_in_function(any_function), **task_init_kwargs
	)


@authenticated_task
def authenticated_create_flow_run(**kwargs):
	"""
	Cria uma execução de flow a partir dos parâmetros passados
	"""
	raise NotImplementedError()  # TODO
	# log(f"Created Flow Run with params: {kwargs}")
	# return create_flow_run.run(**kwargs)


@authenticated_task
def authenticated_wait_for_flow_run(**kwargs):
	"""
	Aguarda uma execução de flow terminar
	"""
	raise NotImplementedError()  # TODO
	# log(f"Waiting Flow Run with params: {kwargs}")
	# return wait_for_flow_run.run(**kwargs)


@authenticated_task
def rename_flow_run(new_name: str):
	ctx = FlowRunContext.get()
	if not ctx:
		log(
			"[rename_flow_run] Erro ao renomear flow_run; FlowRunContext inexistente",
			level="warning",
		)
		return
	if not ctx.flow_run:
		log(
			"[rename_flow_run] Erro ao renomear flow_run; FlowRunContext.flow_run inexistente",
			level="warning",
		)
		return

	async def update():
		async with get_client() as client:
			await client.update_flow_run(flow_run_id=ctx.flow_run.id, name=new_name)

	asyncio.run(update())


def flow_config(
	flow: Flow,
	schedules: list[Schedule] = None,
	dockerfile: str = None,
	memory: Literal["small", "medium", "large"] = "small",
	mount_gcs: bool = False,
) -> dict:
	"""
	Retorna uma configuração de flow, a ser usada na variável
	`_flows`: `_flows = [ flow_config(...), ... ]`

	Args:
		flow(Flow):
			O flow a ser executado.
		schedules(list[Schedule]):
			Lista de schedules para o flow; pode ser vazia/None.
		dockerfile(str):
			Caminho do Dockerfile customizado que executa o flow.
			Pode ser vazio/None. Ex.: `"./pipelines/datalake/..."`
		memory(Literal["small", "medium", "large"]):
			Quantidade de memória RAM disponibilizada para a VM
			executando o flow. Atenção: em Google Cloud Run Jobs,
			não existe disco rígido; o filesystem reside na
			própria memória RAM.
			* Para `memory="small"` (valor padrão), é alocado 4 GB
				de RAM, ideal para flows que não fazem escrita de
				muitos dados em "disco".
			* Para `memory="medium"`, são alocados 12 GB de RAM
			* Para `memory="large"`, são alocados 24 GB de RAM
		mount_gcs(bool):
			Flag indicando se um bucket do GCS deve ser montado
			em `/mnt/gcs` ou não. Como não há disco, se for
			necessário escrever arquivos maiores que a RAM
			disponível, é necessário usar um bucket externo.
			Falso por padrão.
	"""
	if not schedules:
		schedules = []

	memory = str(memory).strip().lower()
	if memory not in ("small", "medium", "large"):
		raise ValueError(f"'{memory}' não é um valor válido para `memory`!")

	return {
		"flow": flow,
		"schedules": schedules,
		"dockerfile": dockerfile,
		"memory": memory,
		"gcs": bool(mount_gcs),
	}


def get_flow_name():
	"""Retorna o nome da flow, especificado em @flow(name='...')"""
	fr_ctx = FlowRunContext.get()
	if not fr_ctx:
		raise RuntimeError("Não foi possível obter 'FlowRunContext'!")
	return fr_ctx.flow.name


def get_normalized_flow_name():
	"""
	Retorna o nome do flow normalizado para uso em pastas etc.
	Ex.: "Report: Previsão do Tempo" → "report_previsao_do_tempo"
	"""
	flow_name = get_flow_name().strip()
	# Normaliza o nome para deploy
	normalized_flow_name = re.sub(
		r"_{2,}",
		"_",
		re.sub(
			r"[^a-z_]",
			"",
			(unicodedata.normalize("NFD", flow_name).lower().replace(" ", "_")),
		),
	)
	if len(normalized_flow_name) < 1:
		raise RuntimeError("Após normalização, nome do flow fica vazio!")
	if is_dev_run():
		return f"{normalized_flow_name}_staging"
	return normalized_flow_name


@authenticated_task
def as_task(function, args: list = None, kwargs: dict = None):
	"""Executa uma função comum como task"""
	args = [] if not args else args
	kwargs = dict() if not kwargs else kwargs

	return function(*args, **kwargs)
