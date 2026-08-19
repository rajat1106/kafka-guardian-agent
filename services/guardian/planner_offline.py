"""Deterministic planner.

This is the default path, not a stub. It runs when there is no API key,
when the anomaly is below the severity floor, and whenever the token budget
is spent — so most incidents in a long-running demo are handled here.

It encodes the causal structure of the simulated world as explicit rules.
That is honest about what it is: a good expert system, not intelligence. It
cannot explain a failure mode nobody anticipated, which is exactly the gap
the LLM planner fills.
"""

from __future__ import annotations

from guardian_platform.config import ClusterCapabilities
from guardian_platform.contracts import (
    Action, ActionType, Diagnosis, Incident, RemediationPlan,
)


def _metrics(incident: Incident) -> dict[str, float]:
    """Most recent value per metric across this incident's anomalies."""
    out: dict[str, float] = {}
    for a in sorted(incident.anomalies, key=lambda x: x.ts):
        out[a.metric] = a.value
    return out


def _breaching(incident: Incident) -> dict[str, float]:
    """Metrics the detector projects will cross their threshold, and when.

    This is the whole point of the trend detector, and diagnosis has to use
    it. Judging a predicted breach by its *current* value defeats the
    prediction: the pool is at 44% precisely because we caught it early, and
    a rule that waits for 80% throws the lead time away and does nothing
    until the incident is already happening.
    """
    out: dict[str, float] = {}
    for a in incident.anomalies:
        if a.detector == "trend_forecast" and a.predicted_breach_seconds is not None:
            # Keep the soonest projection per metric.
            prior = out.get(a.metric)
            if prior is None or a.predicted_breach_seconds < prior:
                out[a.metric] = a.predicted_breach_seconds
    return out


def _urgency(seconds: float | None) -> str:
    if seconds is None:
        return "already breached"
    if seconds < 45:
        return f"projected to breach in ~{seconds:.0f}s"
    return f"trending toward breach in ~{seconds:.0f}s"


def _has(incident: Incident, metric: str) -> bool:
    return any(a.metric == metric for a in incident.anomalies)


def diagnose(incident: Incident, current: dict, caps: ClusterCapabilities) -> Diagnosis:
    m = _metrics(incident)
    breach = _breaching(incident)
    evidence = [f"{a.metric}={a.value:.2f} ({a.detector}): {a.description}"
                for a in incident.anomalies[-6:]]

    # Order matters: the most specific and most consequential first.
    if _has(incident, "healthy"):
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="region_unavailable",
            confidence=0.88,
            reasoning=(
                f"{incident.service} is reporting unhealthy with throughput at "
                "zero. A service that is down cannot be scaled or tuned back to "
                "life; the only remedy that restores traffic is a failover."
            ),
            evidence=evidence, source="offline",
        )

    if _has(incident, "under_replicated_partitions"):
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="partition_replication_degraded",
            confidence=0.82,
            reasoning=(
                "Partitions are under-replicated. Throughput may look healthy, "
                "but the cluster is one broker failure away from data loss, so "
                "this is urgent despite benign-looking traffic metrics."
                + ("" if caps.can_restart_broker else
                   " Broker operations are unavailable on this managed cluster, "
                   "so this requires human escalation.")
            ),
            evidence=evidence, source="offline",
        )

    # ── connection pool ──────────────────────────────────────────────
    pool_util = m.get("db_pool_utilisation", 0.0)
    pool_eta = breach.get("db_pool_utilisation")
    pool_predicted = "db_pool_utilisation" in breach
    if pool_util >= 0.80 or pool_predicted or (
        _has(incident, "p99_latency_ms") and pool_util >= 0.7
    ):
        # Confidence is lower for a projection than for an observed breach —
        # a trend can still bend — but acting early is the point.
        confident = pool_util >= 0.80
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="db_connection_pool_exhaustion",
            confidence=0.85 if confident else 0.76,
            reasoning=(
                f"Connection pool utilisation is {pool_util:.0%} and "
                f"{_urgency(pool_eta)}. Past ~85% queueing for a connection "
                "becomes super-linear, which drives the latency and error-rate "
                "rise that follows. Acting now, before the knee, is cheaper "
                "than recovering after it."
            ) if not confident else (
                f"Connection pool utilisation is {pool_util:.0%}. Past ~85% "
                "queueing for a connection becomes super-linear, which is the "
                "signature of the observed latency and error-rate rise. The "
                "pool is the constraint, not the consumers."
            ),
            evidence=evidence, source="offline",
        )

    # ── consumer capacity ────────────────────────────────────────────
    lag = m.get("consumer_lag", 0.0)
    mem = m.get("memory_used_pct", 0.0)
    mem_eta = breach.get("memory_used_pct")
    mem_predicted = "memory_used_pct" in breach
    lag_predicted = "consumer_lag" in breach

    if (lag > 0 or lag_predicted) and (mem >= 70 or mem_predicted):
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="consumer_capacity_shortfall",
            confidence=0.83 if mem >= 70 else 0.78,
            reasoning=(
                f"Consumer lag is {lag:.0f} and heap is at {mem:.0f}%, "
                f"{_urgency(mem_eta)}. Buffered records for an unprocessed "
                "backlog are what consume the heap, so heap is a symptom of "
                "lag. Restarting clears the heap but not the cause; adding "
                "consumer capacity clears both — and doing it before the OOM "
                "avoids the outage entirely."
            ),
            evidence=evidence, source="offline",
        )

    # Heap climbing with NO backlog is a different failure from a capacity
    # shortfall, and the remedies diverge: capacity fixes a backlog, only a
    # restart reclaims leaked memory. Distinguishing them is the whole reason
    # to look at lag and heap together rather than alarming on heap alone.
    if (mem >= 70 or mem_predicted) and lag <= 0 and not lag_predicted:
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="memory_leak_suspected",
            confidence=0.68,
            reasoning=(
                f"Heap is at {mem:.0f}% and {_urgency(mem_eta)}, but consumer "
                "lag is zero — there is no backlog whose buffered records could "
                "account for the growth. That points to a leak rather than a "
                "capacity shortfall, and adding consumers would not help. A "
                "restart reclaims the heap, but confidence is deliberately "
                "moderate because a leak is inferred from the absence of a "
                "cause rather than observed directly."
            ),
            evidence=evidence, source="offline",
        )

    if lag > 0 or lag_predicted:
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="consumer_capacity_shortfall",
            confidence=0.78,
            reasoning=(
                f"Consumer lag is {lag:.0f} and climbing while heap remains "
                "healthy — arrival rate exceeds processing rate. This is a "
                "throughput shortfall rather than a memory problem."
            ),
            evidence=evidence, source="offline",
        )

    if m.get("error_rate", 0) > 0.05 or m.get("p99_latency_ms", 0) > 500:
        return Diagnosis(
            incident_id=incident.incident_id,
            root_cause="service_degradation",
            confidence=0.6,
            reasoning=(
                "Latency and error rate are elevated without a clear resource "
                "constraint in the observed metrics. Cause is not established "
                "from telemetry alone."
            ),
            evidence=evidence, source="offline",
        )

    return Diagnosis(
        incident_id=incident.incident_id,
        root_cause="unclassified_anomaly",
        confidence=0.35,
        reasoning=(
            "Metrics deviate from baseline but match no known failure "
            "signature. Deliberately low confidence: the policy layer will "
            "route anything consequential to a human."
        ),
        evidence=evidence, source="offline",
    )


def plan(
    incident: Incident,
    diagnosis: Diagnosis,
    current: dict,
    caps: ClusterCapabilities,
) -> RemediationPlan:
    svc = incident.service
    cause = diagnosis.root_cause
    replicas = int(current.get("replicas", 2))
    partitions = int(current.get("partition_count", 6))
    pool = int(current.get("db_pool_size", 20))

    if cause == "region_unavailable":
        return RemediationPlan(
            incident_id=incident.incident_id,
            actions=[Action(
                type=ActionType.FAILOVER_REGION, target=svc,
                params={"to": "us-west-2"},
                rationale="Only a failover restores traffic for a downed region.",
                reversible=True,
            )],
            expected_outcome=f"{svc} healthy again in the failover region",
            verification_metric="healthy",
        )

    if cause == "partition_replication_degraded":
        if not caps.can_restart_broker:
            # The capability-aware branch: on Confluent Cloud, proposing a
            # broker roll would be denied by policy, so propose nothing and
            # let the incident escalate with an accurate explanation.
            return RemediationPlan(
                incident_id=incident.incident_id,
                actions=[Action(
                    type=ActionType.NO_OP, target=svc,
                    rationale=(
                        "Under-replicated partitions need broker-level "
                        "intervention, which is unavailable on a managed "
                        "cluster. Escalating to a human operator."
                    ),
                    reversible=True,
                )],
                expected_outcome="human operator engages Confluent support",
                verification_metric="under_replicated_partitions",
            )
        return RemediationPlan(
            incident_id=incident.incident_id,
            actions=[Action(
                type=ActionType.ROLL_BROKER, target=svc,
                rationale="Roll the degraded broker so replicas resynchronise.",
                reversible=True,
            )],
            expected_outcome="under-replicated partition count returns to zero",
            verification_metric="under_replicated_partitions",
        )

    if cause == "db_connection_pool_exhaustion":
        return RemediationPlan(
            incident_id=incident.incident_id,
            actions=[Action(
                type=ActionType.ADJUST_DB_POOL, target=svc,
                params={"size": min(pool * 3, 200)},
                rationale=(
                    f"Enlarge the pool from {pool} to {min(pool * 3, 200)} and "
                    "recycle leaked connections, moving utilisation back below "
                    "the super-linear queueing region."
                ),
                reversible=True,
            )],
            expected_outcome="p99 latency returns to baseline, errors clear",
            verification_metric="p99_latency_ms",
        )

    if cause == "consumer_capacity_shortfall":
        # Partition count caps useful consumer count — scaling beyond it adds
        # idle pods. If we are already at the cap, add partitions first.
        if replicas >= partitions:
            return RemediationPlan(
                incident_id=incident.incident_id,
                actions=[Action(
                    type=ActionType.INCREASE_PARTITIONS, target=svc,
                    params={"partitions": partitions * 2},
                    rationale=(
                        f"{replicas} consumers already match {partitions} "
                        "partitions, so more consumers would idle. Raising the "
                        "partition count first is what unlocks more parallelism."
                    ),
                    reversible=False,
                )],
                expected_outcome="headroom to add consumers",
                verification_metric="consumer_lag",
            )
        target = min(replicas * 2, partitions)
        return RemediationPlan(
            incident_id=incident.incident_id,
            actions=[Action(
                type=ActionType.SCALE_CONSUMER_GROUP, target=svc,
                params={"replicas": target},
                rationale=(
                    f"Raise consumers from {replicas} to {target} (capped at the "
                    f"{partitions}-partition limit) so processing rate exceeds "
                    "arrival rate and the backlog drains."
                ),
                reversible=True,
            )],
            expected_outcome="consumer lag drains and heap pressure subsides",
            verification_metric="consumer_lag",
        )

    if cause == "memory_leak_suspected":
        return RemediationPlan(
            incident_id=incident.incident_id,
            actions=[Action(
                type=ActionType.RESTART_SERVICE, target=svc,
                rationale=(
                    "Restart reclaims leaked heap. This is blast radius 3, so "
                    "it parks for human approval rather than auto-executing — "
                    "correct for an inferred cause with a brief outage cost."
                ),
                reversible=True,
            )],
            expected_outcome="heap returns to its baseline working set",
            verification_metric="memory_used_pct",
        )

    if cause == "service_degradation":
        return RemediationPlan(
            incident_id=incident.incident_id,
            actions=[Action(
                type=ActionType.NO_OP, target=svc,
                rationale=(
                    "Latency and errors are elevated but telemetry does not "
                    "identify a constraint to act on. Escalating rather than "
                    "guessing at a remedy."
                ),
                reversible=True,
            )],
            expected_outcome="human operator investigates",
            verification_metric=None,
        )

    return RemediationPlan(
        incident_id=incident.incident_id,
        actions=[Action(
            type=ActionType.NO_OP, target=svc,
            rationale=(
                "No confident remedy for an unclassified anomaly. Taking no "
                "action is better than guessing at a live system."
            ),
            reversible=True,
        )],
        expected_outcome="incident escalated for human review",
    )
