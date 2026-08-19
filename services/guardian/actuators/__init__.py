"""Actuator selection."""

from __future__ import annotations

import os

from .base import Actuator, CompositeActuator, ExecutionResult
from .kafka_admin import KafkaAdminActuator
from .simulated import SimulatedActuator


def build_actuator(mode: str | None = None) -> CompositeActuator:
    """Compose the actuator stack for the configured mode.

    Kafka admin operations come first in every mode: they are real against
    whichever cluster is configured, so there is no reason to simulate them.
    """
    mode = (mode or os.getenv("ACTUATOR_MODE", "sim")).lower()
    backends: list[Actuator] = [KafkaAdminActuator()]
    if mode == "k8s":
        from .kubernetes import KubernetesActuator
        backends.append(KubernetesActuator())
    else:
        backends.append(SimulatedActuator())
    return CompositeActuator(backends)


__all__ = [
    "Actuator", "CompositeActuator", "ExecutionResult",
    "KafkaAdminActuator", "SimulatedActuator", "build_actuator",
]
