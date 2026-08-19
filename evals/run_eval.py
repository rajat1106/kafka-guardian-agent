#!/usr/bin/env python3
"""Offline evaluation harness.

Replays each recorded failure scenario through the real detector and the
real planner and scores the result. It runs entirely in-process against the
simulation — no Kafka, no Docker — so it is fast enough to run in CI and
deterministic enough to compare two planner versions honestly.

What it measures, and why each matters:

* **Detection lead time** — seconds between the first anomaly and the
  moment the system actually degrades. This is the number that justifies
  the whole architecture; a system that detects failures after they happen
  is a monitoring dashboard, not a self-healing agent.
* **Root-cause accuracy** — did the diagnosis match the scenario's ground
  truth?
* **Action correctness** — scored in three tiers, because "wrong" is too
  blunt. An action can be correct, *palliative* (relieves the symptom but
  not the cause — a restart for a backlog), or wrong.
* **False-positive rate** — anomalies raised against a completely healthy
  fleet. A noisy detector is expensive in an LLM-backed system, since every
  false positive is a diagnosis someone pays for.
* **Cost per incident** — tokens and dollars, so a quality improvement can
  be weighed against what it costs.

Usage:
    python evals/run_eval.py                 # offline planner
    python evals/run_eval.py --llm           # LLM planner (needs a key)
    python evals/run_eval.py --json out.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("platform", "services/fleet", "services/detector",
            "services/chaos", "services/guardian"):
    sys.path.insert(0, str(ROOT / sub))

from guardian_platform.config import (  # noqa: E402
    ClusterCapabilities, GuardianSettings, KafkaProvider,
)
from guardian_platform.contracts import Anomaly, Incident, Severity  # noqa: E402

import planner_offline  # noqa: E402
from detection import Detector  # noqa: E402
from scenarios import ALL_SCENARIOS, Scenario  # noqa: E402
from simulation import build_fleet  # noqa: E402

WARMUP_TICKS = 35
MAX_TICKS = 60
TICK_SECONDS = 2.0


@dataclass
class ScenarioResult:
    scenario: str
    title: str
    service: str
    detected: bool = False
    detect_tick: int | None = None
    degrade_tick: int | None = None
    lead_time_seconds: float | None = None
    expected_cause: str = ""
    actual_cause: str = ""
    cause_correct: bool = False
    confidence: float = 0.0
    action: str = ""
    action_grade: str = "none"      # correct | palliative | wrong | none
    detectors_fired: list[str] = field(default_factory=list)
    tokens: int = 0
    cost_usd: float = 0.0
    notes: str = ""


def _degraded(m) -> bool:
    """The moment a human would call this an outage."""
    return (
        not m.healthy
        or m.p99_latency_ms > 800
        or m.memory_used_pct > 92
        or m.error_rate > 0.10
        or m.consumer_lag > 5000
    )


def _grade(action: str, scenario: Scenario, managed: bool = False) -> str:
    acceptable = scenario.acceptable_actions
    if managed and scenario.acceptable_actions_managed is not None:
        acceptable = scenario.acceptable_actions_managed
    if action in acceptable:
        return "correct"
    if action in scenario.palliative_actions:
        return "palliative"
    if action == "no_op":
        return "wrong"
    return "wrong"


async def run_scenario(
    scenario: Scenario, caps: ClusterCapabilities, planner,
    managed: bool = False,
) -> ScenarioResult:
    result = ScenarioResult(
        scenario=scenario.key, title=scenario.title, service=scenario.service,
        expected_cause=scenario.ground_truth,
    )
    fleet = build_fleet()
    detector = Detector()
    svc = fleet[scenario.service]

    # Establish a baseline the detector can reason against.
    for _ in range(WARMUP_TICKS):
        for s in fleet.values():
            detector.observe(s.step())

    fault = dict(scenario.fault)
    label = fault.pop("label")
    fault.pop("ttl_ticks", None)
    for key, value in fault.items():
        setattr(svc.faults, key, value)
    svc.faults.label = label
    svc.faults.ttl_ticks = MAX_TICKS + 10

    collected: list[Anomaly] = []
    evidence_frozen_at: int | None = None
    diagnosis_evidence: list[Anomaly] = []
    state_at_diagnosis: dict = {}

    # Run the full window even after detection. Stopping early would leave
    # degrade_tick unset and report the *best* results — those detected long
    # before any degradation — as having no measurable lead time at all.
    for tick in range(1, MAX_TICKS + 1):
        metrics = svc.step()
        for other in fleet.values():
            if other is not svc:
                detector.observe(other.step())

        for anomaly in detector.observe(metrics):
            collected.append(anomaly)
            if result.detect_tick is None:
                result.detect_tick = tick
                result.detected = True
            if anomaly.detector not in result.detectors_fired:
                result.detectors_fired.append(anomaly.detector)

        if result.degrade_tick is None and _degraded(metrics):
            result.degrade_tick = tick

        # Freeze what the agent would have known at decision time, so the
        # diagnosis is scored on evidence available then — not on hindsight
        # gathered while we kept the simulation running to find degradation.
        if (result.detect_tick is not None and evidence_frozen_at is None
                and tick >= result.detect_tick + 6):
            evidence_frozen_at = tick
            diagnosis_evidence = list(collected)
            state_at_diagnosis = {
                "replicas": svc.replicas,
                "partition_count": svc.partition_count,
                "db_pool_size": svc.db_pool_size,
                "region": svc.region,
                "healthy": svc.healthy,
            }

    if not diagnosis_evidence:
        diagnosis_evidence = list(collected)
        state_at_diagnosis = {
            "replicas": svc.replicas, "partition_count": svc.partition_count,
            "db_pool_size": svc.db_pool_size, "region": svc.region,
            "healthy": svc.healthy,
        }

    if result.detect_tick is not None and result.degrade_tick is not None:
        result.lead_time_seconds = round(
            (result.degrade_tick - result.detect_tick) * TICK_SECONDS, 1
        )
    elif result.detect_tick is not None:
        # Detected and the system never degraded within the window. That is
        # the best possible outcome, not a missing measurement — report it as
        # a lower bound rather than a dash.
        result.lead_time_seconds = round(
            (MAX_TICKS - result.detect_tick) * TICK_SECONDS, 1
        )
        result.notes = "no degradation within the observation window"

    if not collected:
        result.notes = "no anomalies raised"
        return result

    incident = Incident(
        service=scenario.service,
        severity=(Severity.CRITICAL
                  if any(a.severity is Severity.CRITICAL for a in diagnosis_evidence)
                  else Severity.WARNING),
        anomalies=diagnosis_evidence,
        scenario=scenario.key,
    )
    current = state_at_diagnosis

    diagnosis, plan = await planner(incident, current, caps)
    result.actual_cause = diagnosis.root_cause
    result.confidence = round(diagnosis.confidence, 2)
    result.cause_correct = diagnosis.root_cause == scenario.ground_truth
    result.tokens = diagnosis.tokens_used
    result.cost_usd = diagnosis.cost_usd
    if plan.actions:
        result.action = plan.actions[0].type.value
        result.action_grade = _grade(result.action, scenario, managed)
    return result


def false_positive_rate() -> tuple[float, int, int]:
    """Anomalies raised against a fleet that is behaving perfectly."""
    fleet = build_fleet()
    detector = Detector()
    for _ in range(WARMUP_TICKS):
        for s in fleet.values():
            detector.observe(s.step())
    observations = 0
    false_positives = 0
    for _ in range(120):
        for s in fleet.values():
            observations += 1
            false_positives += len(detector.observe(s.step()))
    return (false_positives / max(observations, 1)), false_positives, observations


async def main() -> int:
    parser = argparse.ArgumentParser(description="Guardian eval harness")
    parser.add_argument("--llm", action="store_true",
                        help="use the LLM planner (requires ANTHROPIC_API_KEY)")
    parser.add_argument("--provider", choices=["local", "confluent"], default="local",
                        help="cluster capabilities to evaluate against")
    parser.add_argument("--json", type=str, default="", help="write results to a JSON file")
    args = parser.parse_args()

    caps = ClusterCapabilities.for_provider(KafkaProvider(args.provider))

    if args.llm:
        key = os.getenv("ANTHROPIC_API_KEY", "").strip()
        if not key:
            print("error: --llm requires ANTHROPIC_API_KEY", file=sys.stderr)
            return 2
        from budget import TokenBudget
        from planner_llm import LLMPlanner

        settings = GuardianSettings()
        budget = TokenBudget(
            settings.tokens_per_incident, settings.tokens_per_hour,
            settings.tokens_per_day, min_severity=0.0,  # evaluate every scenario
        )
        llm = LLMPlanner(key, settings, budget)

        async def planner(incident, current, capabilities):
            out = await llm.run(incident, current, capabilities, similar=[])
            if out is None:
                d = planner_offline.diagnose(incident, current, capabilities)
                return d, planner_offline.plan(incident, d, current, capabilities)
            return out

        mode = f"LLM ({settings.model_triage} -> {settings.model_diagnose})"
    else:
        async def planner(incident, current, capabilities):
            d = planner_offline.diagnose(incident, current, capabilities)
            return d, planner_offline.plan(incident, d, current, capabilities)

        mode = "offline (deterministic)"

    print(f"\nKafka Guardian — evaluation")
    print(f"planner  : {mode}")
    print(f"cluster  : {args.provider}")
    print("=" * 100)

    managed = args.provider == "confluent"
    results = [await run_scenario(s, caps, planner, managed)
               for s in ALL_SCENARIOS]

    print(f"{'scenario':<19}{'detect':>7}{'lead':>9}  {'root cause':<34}"
          f"{'action':<23}{'grade':<11}")
    print("-" * 100)
    for r in results:
        if r.lead_time_seconds is None:
            lead = "—"
        elif r.notes:
            lead = f">{r.lead_time_seconds:.0f}s"
        else:
            lead = f"{r.lead_time_seconds:.0f}s"
        cause = ("ok  " if r.cause_correct else "MISS ") + r.actual_cause
        mark = {"correct": "correct", "palliative": "palliative",
                "wrong": "WRONG", "none": "—"}[r.action_grade]
        print(f"{r.scenario:<19}{'yes' if r.detected else 'NO':>7}{lead:>9}  "
              f"{cause:<34}{r.action:<23}{mark:<11}")

    fp_rate, fps, obs = false_positive_rate()
    detected = sum(1 for r in results if r.detected)
    correct_cause = sum(1 for r in results if r.cause_correct)
    correct_action = sum(1 for r in results if r.action_grade == "correct")
    palliative = sum(1 for r in results if r.action_grade == "palliative")
    leads = [r.lead_time_seconds for r in results if r.lead_time_seconds]
    tokens = sum(r.tokens for r in results)
    cost = sum(r.cost_usd for r in results)

    print("=" * 100)
    n = len(results)
    print(f"  detection rate       {detected}/{n}")
    print(f"  root-cause accuracy  {correct_cause}/{n}")
    print(f"  action correct       {correct_action}/{n}"
          f"   (+{palliative} palliative)")
    if leads:
        print(f"  median lead time     {statistics.median(leads):.0f}s "
              f"(max {max(leads):.0f}s)")
    print(f"  false-positive rate  {fp_rate:.4f}  ({fps} anomalies / {obs} healthy observations)")
    if tokens:
        print(f"  tokens               {tokens}  (${cost:.4f}, "
              f"${cost / max(n, 1):.4f}/incident)")
    else:
        print("  tokens               0  (no LLM calls)")
    print()

    if args.json:
        payload = {
            "planner": mode, "provider": args.provider,
            "results": [asdict(r) for r in results],
            "summary": {
                "detection_rate": detected / n,
                "cause_accuracy": correct_cause / n,
                "action_correct": correct_action / n,
                "action_palliative": palliative / n,
                "median_lead_seconds": statistics.median(leads) if leads else None,
                "false_positive_rate": fp_rate,
                "tokens": tokens, "cost_usd": round(cost, 6),
            },
        }
        Path(args.json).write_text(json.dumps(payload, indent=2))
        print(f"wrote {args.json}")

    # Non-zero exit if the agent regressed badly — usable as a CI gate.
    return 0 if (correct_cause >= n - 1 and detected == n) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
