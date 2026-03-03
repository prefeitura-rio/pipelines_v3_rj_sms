# -*- coding: utf-8 -*-
from typing import Callable, List, Optional
from prefect.flows import (
  Flow as OriginalFlow,
  FlowDecorator as OriginalFlowDecorator
)


class Flow(OriginalFlow):
	def __init__(self, *args, owners: Optional[List[str]] = None, **kwargs):
		# Guarda a lista de owners como atributo
		self.owners = owners or []
		# Continua chamando o __init__ do Flow original
		super().__init__(*args, **kwargs)

	def get_owners(self):
		return self.owners


class FlowDecorator(OriginalFlowDecorator):
	def __init__(self, *args, **kwargs):
		pass

	def __call__(self, *args, state_handlers: List[Callable] = None, owners: Optional[List[str]] = None, **kwargs):
		return Flow(
			*args,
			owners=owners,
			on_completion=state_handlers,
			on_failure=state_handlers,
			on_cancellation=state_handlers,
			on_crashed=state_handlers,
			on_running=state_handlers,
			validate_parameters=False,
			**kwargs
		)
