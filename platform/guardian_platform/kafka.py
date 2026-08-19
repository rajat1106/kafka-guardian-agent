"""Kafka transport.

Thin async wrappers over aiokafka that (a) build the right client kwargs
for whichever provider is configured, and (b) serialise/deserialise the
Pydantic contracts.  Services never construct an aiokafka client directly.
"""

from __future__ import annotations

import asyncio
import json
import ssl
from typing import Any, AsyncIterator, Iterable, Type, TypeVar

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError
from pydantic import BaseModel

from .config import KafkaProvider, KafkaSettings, kafka_settings
from .topics import ALL_TOPICS, TopicSpec

log = structlog.get_logger(__name__)

M = TypeVar("M", bound=BaseModel)


def client_kwargs(settings: KafkaSettings | None = None) -> dict[str, Any]:
    """Build aiokafka connection kwargs for the configured provider.

    This is the only place in the codebase that knows SASL_SSL exists.
    """
    s = settings or kafka_settings()
    kwargs: dict[str, Any] = {
        "bootstrap_servers": s.bootstrap_servers,
        "security_protocol": s.security_protocol,
        "request_timeout_ms": s.request_timeout_ms,
        "connections_max_idle_ms": s.connections_max_idle_ms,
    }
    if s.uses_sasl:
        if not (s.sasl_username and s.sasl_password):
            raise ValueError(
                f"security_protocol={s.security_protocol} requires "
                "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD "
                "(on Confluent Cloud these are the cluster API key and secret)."
            )
        kwargs["sasl_mechanism"] = s.sasl_mechanism or "PLAIN"
        kwargs["sasl_plain_username"] = s.sasl_username
        kwargs["sasl_plain_password"] = s.sasl_password
        # Confluent Cloud brokers present a public CA cert; the system trust
        # store is sufficient and avoids shipping a bundle in the image.
        if "SSL" in s.security_protocol.upper():
            kwargs["ssl_context"] = ssl.create_default_context()
    return kwargs


async def ensure_topics(
    settings: KafkaSettings | None = None,
    specs: Iterable[TopicSpec] = ALL_TOPICS,
    retries: int = 30,
) -> None:
    """Create the topic set if absent.

    Confluent Cloud enforces a minimum replication factor of 3 and applies
    per-cluster partition quotas, so both values come from settings rather
    than being hardcoded.  Topics that already exist are left untouched —
    this is safe to run on every boot.
    """
    s = settings or kafka_settings()
    admin = AIOKafkaAdminClient(client_id="guardian-bootstrap", **client_kwargs(s))

    for attempt in range(retries):
        try:
            await admin.start()
            break
        except KafkaConnectionError:
            if attempt == retries - 1:
                raise
            log.info("waiting_for_kafka", target=s.describe(), attempt=attempt + 1)
            await asyncio.sleep(2)

    try:
        existing = set(await admin.list_topics())
        wanted = [
            NewTopic(
                name=s.topic(spec.name),
                num_partitions=spec.partitions,
                replication_factor=s.replication_factor,
                topic_configs={
                    "retention.ms": str(spec.retention_ms),
                    "cleanup.policy": spec.cleanup_policy,
                },
            )
            for spec in specs
            if s.topic(spec.name) not in existing
        ]
        if not wanted:
            log.info("topics_ready", target=s.describe(), count=len(list(specs)))
            return
        try:
            await admin.create_topics(wanted)
            log.info(
                "topics_created",
                target=s.describe(),
                topics=[t.name for t in wanted],
                replication_factor=s.replication_factor,
            )
        except TopicAlreadyExistsError:
            # Another service won the race. Fine.
            pass
    finally:
        await admin.close()


class EventProducer:
    """JSON producer for Pydantic contracts."""

    def __init__(self, client_id: str, settings: KafkaSettings | None = None) -> None:
        self._settings = settings or kafka_settings()
        self._client_id = client_id
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> "EventProducer":
        self._producer = AIOKafkaProducer(
            client_id=self._client_id,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
            # `acks="all"` is the only correct setting when the payload is an
            # audit record. Confluent Cloud defaults to this; be explicit so
            # the local cluster behaves identically.
            acks="all",
            enable_idempotence=True,
            linger_ms=20,
            **client_kwargs(self._settings),
        )
        await self._producer.start()
        return self

    async def send(self, spec: TopicSpec, model: BaseModel, key: str | None = None) -> None:
        if self._producer is None:
            raise RuntimeError("producer not started")
        await self._producer.send_and_wait(
            self._settings.topic(spec.name),
            model.model_dump(mode="json"),
            key=key,
        )

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def __aenter__(self) -> "EventProducer":
        return await self.start()

    async def __aexit__(self, *_: object) -> None:
        await self.stop()


class EventConsumer:
    """JSON consumer that yields parsed Pydantic models."""

    def __init__(
        self,
        specs: Iterable[TopicSpec],
        group_id: str,
        model: Type[M],
        settings: KafkaSettings | None = None,
        from_beginning: bool = False,
    ) -> None:
        self._settings = settings or kafka_settings()
        self._specs = list(specs)
        self._group_id = group_id
        self._model = model
        self._offset_reset = "earliest" if from_beginning else "latest"
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> "EventConsumer":
        self._consumer = AIOKafkaConsumer(
            *[self._settings.topic(s.name) for s in self._specs],
            group_id=self._group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset=self._offset_reset,
            enable_auto_commit=True,
            **client_kwargs(self._settings),
        )
        await self._consumer.start()
        return self

    async def __aiter__(self) -> AsyncIterator[M]:
        if self._consumer is None:
            raise RuntimeError("consumer not started")
        async for msg in self._consumer:
            try:
                yield self._model.model_validate(msg.value)
            except Exception as exc:  # noqa: BLE001 - a poison message must not kill the loop
                log.warning("undeserialisable_message", topic=msg.topic, error=str(exc))

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def __aenter__(self) -> "EventConsumer":
        return await self.start()

    async def __aexit__(self, *_: object) -> None:
        await self.stop()
