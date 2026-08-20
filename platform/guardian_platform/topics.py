"""Topic catalogue.

Names here are *logical*; `KafkaSettings.topic()` applies the cluster
prefix, so the same code targets a dedicated local cluster and a shared
Confluent Cloud one.
"""

from __future__ import annotations

from dataclasses import dataclass

from .config import KafkaSettings


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    # Retention is set explicitly because the defaults differ between a
    # self-hosted cluster and Confluent Cloud.
    retention_ms: int = 24 * 60 * 60 * 1000
    cleanup_policy: str = "delete"
    description: str = ""


TELEMETRY_METRICS = TopicSpec(
    "telemetry.metrics", partitions=6, retention_ms=6 * 60 * 60 * 1000,
    description="Raw per-service metric windows from the demo fleet.",
)
TELEMETRY_EVENTS = TopicSpec(
    "telemetry.events", partitions=3,
    description="Discrete lifecycle events: deploys, restarts, chaos injections.",
)
TELEMETRY_CHANGES = TopicSpec(
    "telemetry.changes", partitions=3, retention_ms=7 * 24 * 60 * 60 * 1000,
    description="Deploys, config changes and scaling events, for correlation.",
)
GUARDIAN_ANOMALIES = TopicSpec(
    "guardian.anomalies", partitions=3,
    description="Detector output. The agent's only wake-up signal.",
)
GUARDIAN_DECISIONS = TopicSpec(
    "guardian.decisions", partitions=3, retention_ms=7 * 24 * 60 * 60 * 1000,
    description="Diagnoses and remediation plans, for the UI and the audit trail.",
)
GUARDIAN_ACTIONS = TopicSpec(
    "guardian.actions", partitions=3, retention_ms=7 * 24 * 60 * 60 * 1000,
    description="Every attempted action with its policy decision. Append-only audit log.",
)
GUARDIAN_APPROVALS = TopicSpec(
    "guardian.approvals", partitions=1,
    description="Human approve/reject responses for parked high-blast-radius plans.",
)
GUARDIAN_OUTCOMES = TopicSpec(
    "guardian.outcomes", partitions=3, retention_ms=30 * 24 * 60 * 60 * 1000,
    description="Closing record per incident. Feeds incident memory and the eval harness.",
)

ALL_TOPICS: tuple[TopicSpec, ...] = (
    TELEMETRY_METRICS,
    TELEMETRY_EVENTS,
    TELEMETRY_CHANGES,
    GUARDIAN_ANOMALIES,
    GUARDIAN_DECISIONS,
    GUARDIAN_ACTIONS,
    GUARDIAN_APPROVALS,
    GUARDIAN_OUTCOMES,
)


def resolved(spec: TopicSpec, settings: KafkaSettings) -> str:
    return settings.topic(spec.name)
