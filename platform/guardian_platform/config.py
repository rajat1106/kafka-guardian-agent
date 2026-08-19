"""Configuration for every Guardian service.

The single most important thing this module does is make the rest of the
codebase indifferent to *which* Kafka it is talking to.  A self-hosted
KRaft broker and a Confluent Cloud cluster differ in three ways that leak
into application code if you let them:

1. **Transport** — PLAINTEXT vs SASL_SSL/PLAIN with an API key pair.
2. **Naming**    — Cloud clusters are usually shared, so topics get a
   prefix to avoid collisions with other teams in the same cluster.
3. **Capability** — you cannot restart a broker you do not own.  Confluent
   Cloud has no broker-level operations at all, and its minimum replication
   factor is 3.  The agent must know this *before* it plans a remediation,
   otherwise it proposes actions that can only fail.

Point (3) is the interesting one: capability is part of configuration, not
an error you discover at execution time.
"""

from __future__ import annotations

from enum import Enum
from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaProvider(str, Enum):
    LOCAL = "local"
    CONFLUENT = "confluent"


class ClusterCapabilities(BaseSettings):
    """What the agent is physically allowed to attempt on this cluster.

    Consulted by the planner (to avoid proposing impossible actions) and
    enforced again by the actuator (in case the planner ignores it).
    """

    can_restart_broker: bool = True
    can_alter_broker_config: bool = True
    can_create_topics: bool = True
    can_increase_partitions: bool = True
    can_reassign_partitions: bool = True
    can_reset_consumer_offsets: bool = True
    can_delete_topics: bool = True
    min_replication_factor: int = 1
    notes: str = ""

    @classmethod
    def for_provider(cls, provider: KafkaProvider) -> "ClusterCapabilities":
        if provider is KafkaProvider.CONFLUENT:
            return cls(
                # Confluent Cloud is fully managed: brokers are not yours.
                can_restart_broker=False,
                can_alter_broker_config=False,
                # These are all available through the standard Admin API,
                # subject to your cluster type's quotas.
                can_create_topics=True,
                can_increase_partitions=True,
                # Reassignment is handled by Confluent's own balancer.
                can_reassign_partitions=False,
                can_reset_consumer_offsets=True,
                can_delete_topics=True,
                min_replication_factor=3,
                notes=(
                    "Confluent Cloud is a managed cluster. Broker-level actions "
                    "are unavailable; scale consumers or adjust topic-level "
                    "settings instead. Partition count can only increase, never "
                    "decrease. Replication factor is fixed at 3."
                ),
            )
        return cls(
            notes=(
                "Self-hosted KRaft cluster. All admin operations available, "
                "including broker restart via the container runtime."
            )
        )


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", extra="ignore")

    provider: KafkaProvider = KafkaProvider.LOCAL
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    topic_prefix: str = ""
    replication_factor: int = 1
    request_timeout_ms: int = 40_000
    # Confluent Cloud terminates idle connections; keep well under its limit.
    connections_max_idle_ms: int = 540_000

    @model_validator(mode="after")
    def _apply_provider_defaults(self) -> "KafkaSettings":
        """Fill in the settings a Confluent user should not have to remember.

        A user who sets KAFKA_PROVIDER=confluent and supplies a key pair gets
        a working SASL_SSL config without also having to know the mechanism
        name and the minimum replication factor.
        """
        if self.provider is KafkaProvider.CONFLUENT:
            if self.security_protocol == "PLAINTEXT":
                self.security_protocol = "SASL_SSL"
            if not self.sasl_mechanism:
                self.sasl_mechanism = "PLAIN"
            if self.replication_factor < 3:
                self.replication_factor = 3
        return self

    @property
    def capabilities(self) -> ClusterCapabilities:
        return ClusterCapabilities.for_provider(self.provider)

    @property
    def uses_sasl(self) -> bool:
        return "SASL" in self.security_protocol.upper()

    def topic(self, name: str) -> str:
        """Apply the cluster's topic prefix.

        Every producer, consumer and admin call in the codebase goes through
        this, so a shared Confluent cluster can namespace the whole system
        with a single env var.
        """
        return f"{self.topic_prefix}{name}"

    def describe(self) -> str:
        target = "Confluent Cloud" if self.provider is KafkaProvider.CONFLUENT else "self-hosted KRaft"
        host = self.bootstrap_servers.split(",")[0]
        return f"{target} @ {host} ({self.security_protocol})"


class GuardianSettings(BaseSettings):
    """Agent behaviour: model routing, token budget, autonomy."""

    model_config = SettingsConfigDict(env_prefix="GUARDIAN_", extra="ignore")

    model_triage: str = "claude-haiku-4-5"
    model_diagnose: str = "claude-opus-5"
    effort: str = "low"

    tokens_per_incident: int = 12_000
    tokens_per_hour: int = 120_000
    tokens_per_day: int = 400_000
    max_turns: int = 6
    llm_min_severity: float = 0.55

    auto_approve_max_blast: int = 2


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    anthropic_api_key: str | None = None
    actuator_mode: str = "sim"
    opa_url: str = "http://opa:8181"
    postgres_dsn: str = "postgresql://guardian:guardian@postgres:5432/guardian"
    chaos_enabled: bool = True
    chaos_interval_seconds: int = 90
    service_name: str = "guardian"


@lru_cache(maxsize=1)
def kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@lru_cache(maxsize=1)
def guardian_settings() -> GuardianSettings:
    return GuardianSettings()


@lru_cache(maxsize=1)
def app_settings() -> AppSettings:
    return AppSettings()
