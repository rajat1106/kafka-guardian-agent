#!/usr/bin/env python3
"""Every diagnosable cause must map to a plan.

A missing branch in `plan()` does not raise — it falls through to the no-op
that exists for genuinely unclassifiable anomalies. The agent then diagnoses
correctly, does nothing, and escalates, which looks like caution rather than
a bug. This asserts the mapping is total, so a cause can never lose its
remedy silently.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "platform"), str(ROOT / "services" / "guardian")]

import planner_offline as P  # noqa: E402
from guardian_platform.config import ClusterCapabilities, KafkaProvider  # noqa: E402
from guardian_platform.contracts import ActionType  # noqa: E402

# cause -> must the plan contain a real action?
EXPECTED: dict[str, bool] = {
    "region_unavailable": True,
    "partition_replication_degraded": True,
    "db_connection_pool_exhaustion": True,
    "consumer_capacity_shortfall": True,
    "memory_leak_suspected": True,
    # These two are correctly no-ops: nothing actionable is established.
    "service_degradation": False,
    "unclassified_anomaly": False,
}


class _Diag:
    def __init__(self, cause: str) -> None:
        self.root_cause = cause
        self.confidence = 0.8


class _Inc:
    incident_id = "inc_test"
    service = "user-service"
    anomalies: list = []


def main() -> int:
    caps = ClusterCapabilities.for_provider(KafkaProvider.LOCAL)
    current = {"replicas": 3, "partition_count": 6, "db_pool_size": 30,
               "latest": {"messages_per_sec": 210.0}}
    failures = []

    for cause, wants_action in EXPECTED.items():
        plan = P.plan(_Inc(), _Diag(cause), current, caps)
        actionable = any(a.type is not ActionType.NO_OP for a in plan.actions)
        if actionable != wants_action:
            failures.append(
                f"{cause}: expected {'an action' if wants_action else 'no-op'}, "
                f"got {[a.type.value for a in plan.actions]}"
            )
            print(f"  FAIL  {cause}")
        else:
            actions = ", ".join(a.type.value for a in plan.actions)
            print(f"  ok    {cause:34} -> {actions}")

    # Every cause the diagnoser can emit must appear above.
    source = (ROOT / "services" / "guardian" / "planner_offline.py").read_text()
    emitted = {
        line.split('root_cause="')[1].split('"')[0]
        for line in source.splitlines() if 'root_cause="' in line
    }
    missing = emitted - set(EXPECTED)
    if missing:
        failures.append(f"diagnoser emits causes with no coverage here: {sorted(missing)}")
        print(f"  FAIL  uncovered causes: {sorted(missing)}")

    if failures:
        print("\n" + "\n".join(failures))
        return 1
    print("\nevery diagnosable cause maps to a plan")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
