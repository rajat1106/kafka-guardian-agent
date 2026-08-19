"""Actuator interface.

An actuator turns a typed `Action` into a change in the world. Three
implementations exist and they compose rather than compete:

* `KafkaAdminActuator` — genuinely real in every mode. Partition increases
  and consumer-group offset resets go through the Kafka Admin API against
  whichever cluster is configured, local or Confluent Cloud.
* `SimulatedActuator` — drives the demo fleet's control API for the
  service-level actions (scale, restart, pool resize).
* `KubernetesActuator` — the same service-level actions against a real
  cluster.

`CompositeActuator` routes each action to whichever backend owns it, which
is what lets the system run half-real: Kafka operations hit a live cluster
while service operations hit the simulator.
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass

from guardian_platform.contracts import Action, ActionType


@dataclass
class ExecutionResult:
    success: bool
    detail: str
    actuator: str
    duration_ms: int = 0


class Actuator(abc.ABC):
    """Executes actions. Must be idempotent where the action type allows."""

    name: str = "abstract"

    @abc.abstractmethod
    def handles(self, action_type: ActionType) -> bool:
        ...

    @abc.abstractmethod
    async def execute(self, action: Action) -> ExecutionResult:
        ...

    async def close(self) -> None:  # pragma: no cover - optional hook
        return None


class CompositeActuator(Actuator):
    """Routes each action to the first backend that claims it."""

    name = "composite"

    def __init__(self, backends: list[Actuator]) -> None:
        self._backends = backends

    def handles(self, action_type: ActionType) -> bool:
        return any(b.handles(action_type) for b in self._backends)

    async def execute(self, action: Action) -> ExecutionResult:
        started = time.perf_counter()
        for backend in self._backends:
            if backend.handles(action.type):
                result = await backend.execute(action)
                if not result.duration_ms:
                    result.duration_ms = int((time.perf_counter() - started) * 1000)
                return result
        return ExecutionResult(
            success=False,
            detail=(f"no actuator claims {action.type.value}; this is a "
                    "configuration gap, not a cluster failure"),
            actuator=self.name,
            duration_ms=int((time.perf_counter() - started) * 1000),
        )

    async def close(self) -> None:
        for b in self._backends:
            await b.close()

    def describe(self) -> str:
        return " + ".join(b.name for b in self._backends)
