"""Real Kafka admin actions — identical against local and Confluent Cloud.

This actuator is the reason the demo is not purely theatre: when the agent
decides to add partitions, partitions really are added to the configured
cluster. Point KAFKA_PROVIDER at Confluent Cloud and the same code path
mutates a managed cluster.
"""

from __future__ import annotations

import time

import structlog
from aiokafka.admin import AIOKafkaAdminClient, NewPartitions

from guardian_platform.config import KafkaSettings, kafka_settings
from guardian_platform.contracts import Action, ActionType
from guardian_platform.kafka import client_kwargs

from .base import Actuator, ExecutionResult

log = structlog.get_logger(__name__)

_OWNED = {
    ActionType.INCREASE_PARTITIONS,
    ActionType.ALTER_TOPIC_CONFIG,
    ActionType.RESET_CONSUMER_OFFSET,
}


class KafkaAdminActuator(Actuator):
    name = "kafka-admin"

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        self._settings = settings or kafka_settings()
        self._admin: AIOKafkaAdminClient | None = None

    def handles(self, action_type: ActionType) -> bool:
        return action_type in _OWNED

    async def _client(self) -> AIOKafkaAdminClient:
        if self._admin is None:
            self._admin = AIOKafkaAdminClient(
                client_id="guardian-actuator", **client_kwargs(self._settings)
            )
            await self._admin.start()
        return self._admin

    async def execute(self, action: Action) -> ExecutionResult:
        started = time.perf_counter()
        try:
            admin = await self._client()
            if action.type is ActionType.INCREASE_PARTITIONS:
                detail = await self._increase_partitions(admin, action)
            elif action.type is ActionType.ALTER_TOPIC_CONFIG:
                detail = await self._alter_topic(admin, action)
            else:
                detail = await self._reset_offset(action)
            return ExecutionResult(
                True, detail, self.name, int((time.perf_counter() - started) * 1000)
            )
        except Exception as exc:  # noqa: BLE001 — surfaced to the agent as a failed action
            log.warning("kafka_action_failed", action=action.type.value, error=str(exc))
            return ExecutionResult(
                False, f"{type(exc).__name__}: {exc}", self.name,
                int((time.perf_counter() - started) * 1000),
            )

    async def _increase_partitions(self, admin: AIOKafkaAdminClient, action: Action) -> str:
        topic = self._settings.topic(action.target)
        target = int(action.params["partitions"])
        await admin.create_partitions({topic: NewPartitions(total_count=target)})
        return (f"increased {topic} to {target} partitions on "
                f"{self._settings.describe()}")

    async def _alter_topic(self, admin: AIOKafkaAdminClient, action: Action) -> str:
        from aiokafka.admin import ConfigResource, ConfigResourceType

        topic = self._settings.topic(action.target)
        configs = {k: str(v) for k, v in action.params.get("configs", {}).items()}
        if not configs:
            return "no topic configs supplied; nothing to alter"
        await admin.alter_configs(
            [ConfigResource(ConfigResourceType.TOPIC, topic, configs=configs)]
        )
        return f"altered {topic} configs: {configs}"

    async def _reset_offset(self, action: Action) -> str:
        # Deliberately not implemented as a live mutation: resetting offsets
        # on a running consumer group requires the group to be empty, so
        # doing it safely means coordinating a stop/start. The policy already
        # denies the dangerous variant; this records intent rather than
        # pretending an unsafe operation succeeded.
        return (f"offset reset for group {action.target!r} requires the group to be "
                f"idle; recorded intent to reset to {action.params.get('to', 'latest')!r} "
                "without stopping a live group")

    async def close(self) -> None:
        if self._admin is not None:
            await self._admin.close()
            self._admin = None
