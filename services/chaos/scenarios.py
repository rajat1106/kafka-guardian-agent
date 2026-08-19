"""Failure scenarios.

The first three are the scenarios from the original blog post; the last
two are Kafka-native failures that belong in a Kafka-centric system.

Each carries a `ground_truth` root cause and a set of `acceptable_actions`.
Those two fields are what turn a demo into an evaluation: the eval harness
replays these scenarios and scores whether the agent's diagnosis matched
the ground truth and whether the action it chose was in the acceptable set.
Without them you can only watch the agent do something and nod.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Scenario:
    key: str
    title: str
    service: str
    description: str
    fault: dict[str, Any]
    ground_truth: str
    acceptable_actions: list[str]
    # Actions that "work" but treat the symptom instead of the cause.
    # Scored as partial credit by the eval harness.
    palliative_actions: list[str] = field(default_factory=list)
    # What counts as correct changes with the cluster. On a managed
    # cluster some remedies are simply unavailable, and escalating is then
    # the right answer rather than a failure to act. When None, the
    # self-hosted set applies to both.
    acceptable_actions_managed: list[str] | None = None
    expected_lead_time_ticks: int = 0


CONSUMER_OOM = Scenario(
    key="consumer_oom",
    title="Consumer heap exhaustion (predicted OOM)",
    service="payment-service",
    description=(
        "A slow downstream call makes the payment consumer fall behind. Lag "
        "accumulates, buffered records grow the heap, and the pod is on course "
        "to OOM-kill in a few minutes. The fix is more consumer capacity, not "
        "a restart — a restart clears the heap but the lag rebuilds immediately."
    ),
    fault={"label": "consumer_oom", "ttl_ticks": 60,
           "consumer_slowdown": 2.6, "memory_leak_mb_per_tick": 9.0},
    ground_truth="consumer_capacity_shortfall",
    acceptable_actions=["scale_consumer_group", "increase_partitions"],
    palliative_actions=["restart_service", "clear_service_backlog"],
    expected_lead_time_ticks=12,
)

POOL_EXHAUSTION = Scenario(
    key="pool_exhaustion",
    title="DB connection pool exhaustion cascade",
    service="user-service",
    description=(
        "Connections leak out of the user-service pool. As utilisation crosses "
        "~85% queuing becomes super-linear, p99 latency jumps an order of "
        "magnitude and the error rate follows. Enlarging or recycling the pool "
        "is the fix; throttling producers only hides it."
    ),
    fault={"label": "pool_exhaustion", "ttl_ticks": 45, "pool_leak_per_tick": 1.1},
    ground_truth="db_connection_pool_exhaustion",
    acceptable_actions=["adjust_db_pool", "restart_service"],
    palliative_actions=["throttle_producer", "scale_consumer_group"],
    expected_lead_time_ticks=9,
)

REGION_OUTAGE = Scenario(
    key="region_outage",
    title="Region outage requiring failover",
    service="notification-service",
    description=(
        "The service's region goes dark: health checks fail and throughput "
        "drops to zero while lag climbs. Only a region failover recovers it, "
        "and failover is high blast radius, so this scenario should park for "
        "human approval rather than auto-execute."
    ),
    fault={"label": "region_outage", "ttl_ticks": 40, "region_down": True,
           "consumer_slowdown": 50.0},
    ground_truth="region_unavailable",
    acceptable_actions=["failover_region"],
    palliative_actions=["restart_service"],
    expected_lead_time_ticks=2,
)

LAG_SPIKE = Scenario(
    key="lag_spike",
    title="Consumer lag explosion under load surge",
    service="user-service",
    description=(
        "A traffic surge outruns consumer capacity. Lag climbs steeply but heap "
        "stays healthy, which distinguishes it from the OOM scenario — the "
        "correct response is capacity, and partitions cap how much capacity "
        "you can add."
    ),
    fault={"label": "lag_spike", "ttl_ticks": 35, "consumer_slowdown": 3.4},
    ground_truth="consumer_capacity_shortfall",
    acceptable_actions=["scale_consumer_group", "increase_partitions"],
    palliative_actions=["clear_service_backlog", "throttle_producer"],
    expected_lead_time_ticks=6,
)

UNDER_REPLICATED = Scenario(
    key="under_replicated",
    title="Under-replicated partitions after broker degradation",
    service="payment-service",
    description=(
        "Partitions fall out of sync — durability is at risk even though "
        "throughput looks fine. On a self-hosted cluster the fix is a broker "
        "roll; on Confluent Cloud brokers are not yours, so the agent must "
        "recognise the action is unavailable and escalate instead."
    ),
    fault={"label": "under_replicated", "ttl_ticks": 30,
           "partition_offline": True, "upstream_latency_ms": 60.0},
    ground_truth="partition_replication_degraded",
    acceptable_actions=["roll_broker", "alter_topic_config"],
    palliative_actions=[],
    # No broker operations exist on Confluent Cloud, so recognising that and
    # escalating is the correct outcome — not a missed remediation.
    acceptable_actions_managed=["no_op", "alter_topic_config"],
    expected_lead_time_ticks=3,
)

ALL_SCENARIOS: tuple[Scenario, ...] = (
    CONSUMER_OOM, POOL_EXHAUSTION, REGION_OUTAGE, LAG_SPIKE, UNDER_REPLICATED,
)
BY_KEY = {s.key: s for s in ALL_SCENARIOS}
