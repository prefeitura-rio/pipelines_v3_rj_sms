# -*- coding: utf-8 -*-
from typing import Callable, List, Optional
from prefect.flows import Flow as OriginalFlow, FlowDecorator as OriginalFlowDecorator


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
