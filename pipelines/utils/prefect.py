# -*- coding: utf-8 -*-
import asyncio
from typing import Any, Callable, Union, List, Optional

from prefect import Task, get_client
from prefect.context import FlowRunContext
from prefect.flows import Flow as OriginalFlow, FlowDecorator as OriginalFlowDecorator

from pipelines.utils.env import get_current_environment
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
	state_handlers: List[Callable] = None
	owners: List[str] = None

	def __init__(
		self,
		*args,
		name: str = None,
		state_handlers: List[Callable] = None,
		owners: Optional[List[str]] = None,
		**kwargs,
	):
		self.name = name
		self.state_handlers = state_handlers or []
		self.owners = owners or []

	def __call__(self, *args, **kwargs):
		return Flow(
			*args,
			name=self.name,
			owners=self.owners,
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


@authenticated_task()
def authenticated_create_flow_run(**kwargs):
	"""
	Cria uma execução de flow a partir dos parâmetros passados
	"""
	raise NotImplementedError()  # TODO
	# log(f"Created Flow Run with params: {kwargs}")
	# return create_flow_run.run(**kwargs)


@authenticated_task()
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
