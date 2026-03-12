# -*- coding: utf-8 -*-
from typing import Any, Callable, Union

from prefect import Task

from pipelines.utils.env import get_current_environment
from pipelines.utils.infisical import inject_bd_credentials
from pipelines.utils.logger import log


def authenticated_task(fn: Callable = None, **task_init_kwargs: Any) -> Union[
	Task, Callable[[Callable], Task]
]:
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
		return Task(
			fn=inject_credential_setting_in_function(fn),
			**task_init_kwargs
		)
	# Decorator
	return lambda any_function: Task(
		fn=inject_credential_setting_in_function(any_function),
		**task_init_kwargs,
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
