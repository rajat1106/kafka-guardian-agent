"""The Guardian agent loop.

    correlate → recall → diagnose → plan → gate → execute → verify → learn

Two properties matter more than the individual steps:

**Every step is journaled.** The agent writes each completed step to
Postgres before moving on, so a crash mid-incident resumes rather than
re-executing. Remediation actions are not safely repeatable — scaling a
consumer group twice because the process died between execution and
recording is exactly the class of bug that makes people distrust
automation.

**The gate is not advisory.** No action reaches an actuator without a
policy decision, and a `require_approval` decision parks the incident until
a human answers on `guardian.approvals`. The agent has no path around it.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

import structlog

from guardian_platform.config import (
    AppSettings, ClusterCapabilities, GuardianSettings, KafkaSettings,
)
from guardian_platform.contracts import (
    Action, ActionResult, ActionType, Anomaly, ApprovalRequest, AutonomyMode,
    Diagnosis, Incident, IncidentState, Outcome, RemediationPlan, Severity,
    ShadowRecord,
)

import planner_offline
import rollback
from actuators import CompositeActuator
from budget import TokenBudget
from memory import IncidentMemory
from planner_llm import LLMPlanner
from policy import PolicyGate

log = structlog.get_logger(__name__)

# How long an incident stays open collecting related anomalies before the
# agent commits to a diagnosis. Long enough to correlate, short enough that
# a predicted breach is still preventable.
CORRELATION_WINDOW_SECONDS = 8.0
APPROVAL_TIMEOUT_SECONDS = 300.0
# Quiet period after an incident closes before the same service may open
# another. Longer than the verification window, so a remedy that is still
# settling does not look like a fresh incident.
POST_INCIDENT_COOLDOWN_SECONDS = 45.0

# Absolute health bounds, used when an incident has no in-incident baseline
# for its verification metric — which happens whenever the metric that opened
# the incident is not the metric the remedy targets. Mirrors the detector's
# thresholds.
HEALTHY_BELOW: dict[str, float] = {
    "consumer_lag": 500.0,
    "memory_used_pct": 85.0,
    "p99_latency_ms": 250.0,
    "error_rate": 0.02,
    "cpu_pct": 85.0,
    "db_pool_utilisation": 0.80,
    "under_replicated_partitions": 1.0,
}


class GuardianAgent:
    def __init__(
        self,
        kafka: KafkaSettings,
        guardian: GuardianSettings,
        app: AppSettings,
        memory: IncidentMemory,
        policy: PolicyGate,
        actuator: CompositeActuator,
        budget: TokenBudget,
        llm: LLMPlanner | None,
        emit,          # async callable(topic_spec, model, key) -> None
        fleet_state,   # async callable(service) -> dict
        autonomy: AutonomyMode = AutonomyMode.SUPERVISED,
        changes=None,   # ChangeTracker | None
    ) -> None:
        self._kafka = kafka
        self._s = guardian
        self._app = app
        self._memory = memory
        self._policy = policy
        self._actuator = actuator
        self._budget = budget
        self._llm = llm
        self._emit = emit
        self._fleet_state = fleet_state

        # Set by the brain supervisor when configuration changes; shown in
        # the dashboard so it is always clear what made a decision.
        self.planner_description = "llm+rules" if llm else "rules engine"
        self.autonomy = autonomy
        self._changes = changes
        self._open: dict[str, Incident] = {}          # service -> incident
        # A fault outlives the incident that responds to it. Without a
        # cooldown the same root cause opens a fresh incident every few
        # seconds, each re-running diagnosis and re-spending tokens.
        self._cooldown_until: dict[str, float] = {}
        self._pending: dict[str, asyncio.Future] = {} # incident_id -> approval
        self._failed_actions: dict[str, int] = {}
        self._tasks: set[asyncio.Task] = set()

    @property
    def capabilities(self) -> ClusterCapabilities:
        return self._kafka.capabilities

    # ── entry point ──────────────────────────────────────────────────
    async def on_anomaly(self, anomaly: Anomaly) -> None:
        """Fold an anomaly into an open incident, or open a new one."""
        existing = self._open.get(anomaly.service)
        if existing is not None:
            existing.anomalies.append(anomaly)
            if anomaly.severity is Severity.CRITICAL:
                existing.severity = Severity.CRITICAL
            return

        cooldown = self._cooldown_until.get(anomaly.service, 0.0)
        remaining = cooldown - time.monotonic()
        if remaining > 0:
            log.debug("anomaly_suppressed_cooldown", service=anomaly.service,
                      metric=anomaly.metric, seconds_remaining=round(remaining, 1))
            return

        incident = Incident(
            service=anomaly.service,
            severity=anomaly.severity,
            anomalies=[anomaly],
            title=f"{anomaly.service}: {anomaly.metric} anomaly",
            state=IncidentState.CORRELATING,
        )
        self._open[anomaly.service] = incident
        await self._memory.open_incident(incident)
        log.info("incident_opened", incident=incident.incident_id,
                 service=incident.service, metric=anomaly.metric,
                 detector=anomaly.detector)

        task = asyncio.create_task(self._run_incident(incident))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def on_approval(self, incident_id: str, approved: bool) -> None:
        fut = self._pending.get(incident_id)
        if fut and not fut.done():
            fut.set_result(approved)

    # ── the loop ─────────────────────────────────────────────────────
    async def _run_incident(self, incident: Incident) -> None:
        try:
            done = await self._memory.completed_steps(incident.incident_id)

            # 1. Correlate — let related anomalies arrive before deciding.
            # Bracket the wait with two reads so the correlation window also
            # yields a measured rate of change. The detector's own anomaly
            # window is a poor source for this: at the moment of detection it
            # is still mostly pre-incident samples, so a slope fitted to it
            # underestimates the deficit and the planner under-provisions.
            before = await self._fleet_state(incident.service)
            await asyncio.sleep(CORRELATION_WINDOW_SECONDS)
            incident.state = IncidentState.DIAGNOSING
            await self._journal(incident, "correlated",
                                {"anomaly_count": len(incident.anomalies)})

            current = await self._fleet_state(incident.service)
            current["rates"] = self._rates(before, current, CORRELATION_WINDOW_SECONDS)

            # 2. Recall.
            similar = await self._memory.similar(incident)
            if similar:
                log.info("recalled_incidents", incident=incident.incident_id,
                         count=len(similar))

            # What changed? The first question a human asks, and until now
            # the one the agent could not.
            change_lines: list[str] = []
            if self._changes is not None:
                change_lines = self._changes.describe(
                    incident.service, incident.opened_at
                )
                if change_lines:
                    log.info("changes_correlated", incident=incident.incident_id,
                             count=len(change_lines), first=change_lines[0][:90])

            # 3+4. Diagnose and plan.
            diagnosis, plan = await self._diagnose(
                incident, current, similar, change_lines
            )
            diagnosis.correlated_changes = change_lines
            incident.diagnosis = diagnosis
            incident.plan = plan
            incident.state = IncidentState.PLANNING
            await self._journal(incident, "diagnosed", {
                "root_cause": diagnosis.root_cause,
                "confidence": diagnosis.confidence,
                "source": diagnosis.source,
                "tokens": diagnosis.tokens_used,
            })
            await self._emit_decision(incident)

            # 5+6. Gate and execute.
            results: list[ActionResult] = await self._execute_plan(
                incident, plan, current, done
            )

            # 7. Verify.
            incident.state = IncidentState.VERIFYING
            executed_any = any(r.executed and r.success for r in results)
            if self.autonomy is AutonomyMode.SHADOW:
                verified, detail = False, (
                    "shadow mode — the plan was evaluated but not executed"
                )
            elif not executed_any:
                # Nothing the agent did took effect, so any recovery is
                # someone else's — a fault that expired, a human acting out of
                # band, load subsiding. Reading the metric here and calling it
                # resolved credits the agent for work it did not do, which
                # corrupts the very statistics this system reports about
                # itself.
                verified, detail = False, (
                    "no action was executed; not claiming a resolution the "
                    "agent did not cause"
                )
            else:
                verified, detail = await self._verify(incident, plan)

            # Undo on failure. An action that did not help is not neutral —
            # it left the system in a state nobody chose.
            rolled_back = False
            if not verified and self.autonomy is not AutonomyMode.SHADOW:
                rolled_back = await self._rollback(incident, results, current)

            # 8. Learn.
            incident.state = (IncidentState.RESOLVED if verified
                              else IncidentState.ESCALATED)
            incident.closed_at = datetime.now(timezone.utc)
            await self._close(incident, results, verified, detail,
                              rolled_back=rolled_back)

        except Exception as exc:  # noqa: BLE001 — one bad incident must not stop the agent
            log.exception("incident_failed", incident=incident.incident_id, error=str(exc))
            incident.state = IncidentState.FAILED
            incident.closed_at = datetime.now(timezone.utc)
            await self._close(incident, [], False, f"agent error: {exc}")
        finally:
            self._open.pop(incident.service, None)
            self._pending.pop(incident.incident_id, None)
            # Give the remedy time to take effect before this service is
            # allowed to open another incident.
            self._cooldown_until[incident.service] = (
                time.monotonic() + POST_INCIDENT_COOLDOWN_SECONDS
            )

    # ── steps ────────────────────────────────────────────────────────
    @staticmethod
    def _rates(before: dict, after: dict, seconds: float) -> dict:
        """Per-second rate of change for the metrics a planner sizes against."""
        b = (before or {}).get("latest") or {}
        a = (after or {}).get("latest") or {}
        rates: dict[str, float] = {}
        for metric in ("consumer_lag", "memory_used_pct", "db_pool_used"):
            if metric in b and metric in a:
                try:
                    rates[metric] = (float(a[metric]) - float(b[metric])) / seconds
                except (TypeError, ValueError):
                    continue
        return rates

    async def _diagnose(
        self, incident: Incident, current: dict, similar: list[dict],
        change_lines: list[str] | None = None,
    ) -> tuple[Diagnosis, RemediationPlan]:
        """LLM path when it clears the budget gate; deterministic otherwise."""
        peak_score = max((a.score for a in incident.anomalies), default=0.0)
        status = self._budget.check(
            incident.incident_id, severity_score=peak_score, estimated_tokens=3500
        )

        if self._llm is not None and status.allowed:
            result = await self._llm.run(incident, current, self.capabilities,
                                         similar, change_lines or [])
            if result is not None:
                return result
            log.info("llm_fell_back", incident=incident.incident_id)
        elif self._llm is not None:
            log.info("llm_skipped", incident=incident.incident_id,
                     verdict=status.verdict, detail=status.detail)

        diagnosis = planner_offline.diagnose(
            incident, current, self.capabilities, change_lines
        )
        return diagnosis, planner_offline.plan(
            incident, diagnosis, current, self.capabilities
        )

    async def _execute_plan(
        self, incident: Incident, plan: RemediationPlan, current: dict, done: set[str]
    ) -> list[ActionResult]:
        results: list[ActionResult] = []
        for index, action in enumerate(plan.actions):
            step = f"action:{index}"
            if step in done:
                log.info("step_already_done", incident=incident.incident_id, step=step)
                continue

            decision = await self._policy.evaluate(
                action=action,
                capabilities=self.capabilities,
                diagnosis=incident.diagnosis,
                current_state=current,
                failed_action_count=self._failed_actions.get(incident.incident_id, 0),
            )
            action.blast_radius = decision.blast_radius

            if decision.effect == "deny":
                log.info("action_denied", incident=incident.incident_id,
                         action=action.type.value, reasons=decision.reasons)
                results.append(ActionResult(
                    incident_id=incident.incident_id, action=action,
                    decision=decision, executed=False, success=False,
                    detail="; ".join(decision.reasons) or "denied by policy",
                ))
                await self._emit_action(results[-1])
                continue

            # Compute the undo before acting: the state it depends on stops
            # existing the moment the action lands.
            action.inverse = rollback.inverse_of(action, current)

            if self.autonomy is AutonomyMode.SHADOW:
                # Gate before the approval wait, not after. Everything above
                # ran for real — detection, diagnosis, planning, the policy
                # verdict — but shadow mode must never block on a human, or a
                # silent evaluation run turns into a queue of approval requests
                # nobody asked for and the record stalls at the first
                # high-blast action.
                await self._record_shadow(incident, action, decision)
                results.append(ActionResult(
                    incident_id=incident.incident_id, action=action,
                    decision=decision, executed=False, success=False,
                    shadowed=True,
                    detail=(f"shadow mode: would have run {action.type.value} "
                            f"on {action.target}"
                            + ("" if decision.effect == "allow"
                               else " after human approval")),
                ))
                await self._emit_action(results[-1])
                log.info("shadow_action", incident=incident.incident_id,
                         action=action.type.value,
                         would_auto_execute=decision.effect == "allow")
                continue

            approved_by_human = False
            if decision.effect == "require_approval":
                incident.state = IncidentState.AWAITING_APPROVAL
                granted = await self._await_approval(incident, plan, action, decision)
                if not granted:
                    results.append(ActionResult(
                        incident_id=incident.incident_id, action=action,
                        decision=decision, executed=False, success=False,
                        detail="not approved by an operator within the timeout",
                    ))
                    await self._emit_action(results[-1])
                    continue
                approved_by_human = True

            incident.state = IncidentState.EXECUTING
            outcome = await self._actuator.execute(action)
            if not outcome.success:
                self._failed_actions[incident.incident_id] = (
                    self._failed_actions.get(incident.incident_id, 0) + 1
                )
            result = ActionResult(
                incident_id=incident.incident_id, action=action, decision=decision,
                executed=True, success=outcome.success, detail=outcome.detail,
                actuator=outcome.actuator, duration_ms=outcome.duration_ms,
            )
            results.append(result)
            # Journal *before* emitting, so a crash cannot lose the record of
            # an action that already changed the world.
            await self._journal(incident, step, {
                "action": action.type.value, "success": outcome.success,
                "detail": outcome.detail, "human_approved": approved_by_human,
            })
            await self._emit_action(result)
            log.info("action_executed", incident=incident.incident_id,
                     action=action.type.value, success=outcome.success,
                     blast_radius=decision.blast_radius, detail=outcome.detail)
        return results

    async def _await_approval(
        self, incident: Incident, plan: RemediationPlan,
        action: Action, decision, timeout: float = APPROVAL_TIMEOUT_SECONDS,
    ) -> bool:
        from guardian_platform.topics import GUARDIAN_DECISIONS

        request = ApprovalRequest(
            incident_id=incident.incident_id, plan_id=plan.plan_id,
            actions=[action], decisions=[decision],
        )
        await self._emit(GUARDIAN_DECISIONS, request, incident.incident_id)
        log.info("awaiting_approval", incident=incident.incident_id,
                 action=action.type.value, blast_radius=decision.blast_radius,
                 reasons=decision.reasons)

        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[incident.incident_id] = fut
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            log.info("approval_timeout", incident=incident.incident_id)
            return False

    async def _verify(self, incident: Incident, plan: RemediationPlan) -> tuple[bool, str]:
        """Did the remediation actually work?

        Re-reads live state rather than trusting the actuator's report. An
        actuator returning success means the API call was accepted, not that
        the system recovered.
        """
        # A no-op is never a resolution. This check must come first: a no-op
        # plan carries no verification metric, so a "no metric -> assume fine"
        # branch ahead of it silently scores every escalation as a success.
        if not plan.actions or all(a.type is ActionType.NO_OP for a in plan.actions):
            return False, "no action was taken; escalated for human review"

        if plan.verification_metric is None:
            return True, "no verification metric specified"

        metric = plan.verification_metric
        await asyncio.sleep(min(plan.verification_window_seconds, 12))
        first = await self._read_metric(incident.service, metric)

        if metric == "healthy":
            # A service coming back is inherently a few seconds behind the
            # action that recovered it — it may still be restarting, and a
            # single sample taken mid-restart reads as a failed remedy.
            if first:
                return True, "healthy=True"
            for _ in range(4):
                await asyncio.sleep(5)
                if await self._read_metric(incident.service, metric):
                    return True, "healthy=True (recovered during verification)"
            return False, "still unhealthy after ~30s; escalating"

        before = next(
            (a.value for a in reversed(incident.anomalies) if a.metric == metric), None
        )
        if first is None:
            return False, f"could not read {metric} after acting; cannot confirm recovery"

        if before is None:
            # The incident was opened by a different metric than the one the
            # remedy targets, so there is no in-incident baseline to compare
            # against. Fall back to an absolute health check rather than
            # assuming success — "no baseline" is not evidence of recovery.
            bound = HEALTHY_BELOW.get(metric)
            if bound is None:
                return False, f"no baseline or health bound for {metric}; escalating"
            ok = first < bound
            return ok, (
                f"{metric} = {first:.2f} vs healthy bound {bound:.2f} "
                f"({'within' if ok else 'above'} bound; no in-incident baseline)"
            )

        # A draining backlog keeps rising for a moment after capacity is added,
        # so an absolute comparison at a single instant fails a remedy that is
        # in fact working. Take a second sample and accept a falling trend as
        # evidence of recovery.
        if first < before * 0.85:
            return True, f"{metric}: {before:.2f} → {first:.2f} (improved)"

        await asyncio.sleep(8)
        second = await self._read_metric(incident.service, metric)
        if second is None:
            return False, f"{metric}: {before:.2f} → {first:.2f} (not improved)"

        if second < first * 0.95:
            return True, (
                f"{metric}: {before:.2f} → {first:.2f} → {second:.2f} "
                "(now falling; remedy is taking effect)"
            )
        if second < before * 0.85:
            return True, f"{metric}: {before:.2f} → {second:.2f} (improved)"
        return False, (
            f"{metric}: {before:.2f} → {first:.2f} → {second:.2f} "
            "(not improving; escalating)"
        )

    async def _read_metric(self, service: str, metric: str) -> float | None:
        """Read one metric from live fleet state."""
        state = await self._fleet_state(service)
        latest = state.get("latest") or {}
        if metric == "healthy":
            return 1.0 if latest.get("healthy") else 0.0
        if metric == "db_pool_utilisation" and metric not in latest:
            size = latest.get("db_pool_size") or 1
            return (latest.get("db_pool_used") or 0) / size
        value = latest.get(metric)
        return float(value) if value is not None else None

    async def _record_shadow(
        self, incident: Incident, action: Action, decision
    ) -> None:
        from guardian_platform.topics import GUARDIAN_DECISIONS

        diagnosis = incident.diagnosis
        record = ShadowRecord(
            incident_id=incident.incident_id,
            service=incident.service,
            root_cause=diagnosis.root_cause if diagnosis else "undiagnosed",
            confidence=diagnosis.confidence if diagnosis else 0.0,
            action_type=action.type.value,
            action_params=action.params,
            blast_radius=decision.blast_radius,
            policy_effect=decision.effect,
            would_have_auto_executed=decision.effect == "allow",
            reasoning=action.rationale,
        )
        await self._emit(GUARDIAN_DECISIONS, record, incident.incident_id)

    async def _rollback(
        self, incident: Incident, results: list[ActionResult], current: dict
    ) -> bool:
        """Undo actions that ran but did not help, newest first."""
        any_rolled = False
        for result in reversed(results):
            if not (result.executed and result.success):
                continue
            inverse = result.action.inverse
            if inverse is None:
                result.rollback_detail = (
                    f"{result.action.type.value} has no automatic undo; "
                    "manual intervention may be required"
                )
                log.info("rollback_unavailable", incident=incident.incident_id,
                         action=result.action.type.value)
                continue

            # The undo is an action like any other and is gated like one. An
            # agent that can bypass policy on the way back has no policy.
            decision = await self._policy.evaluate(
                action=inverse, capabilities=self.capabilities,
                diagnosis=incident.diagnosis, current_state=current,
                failed_action_count=0,
            )
            if decision.effect == "deny":
                result.rollback_detail = (
                    f"undo refused by policy: {'; '.join(decision.reasons)}"
                )
                continue
            if decision.effect == "require_approval":
                result.rollback_detail = (
                    "undo needs human approval; left in place for an operator"
                )
                continue

            outcome = await self._actuator.execute(inverse)
            result.rolled_back = outcome.success
            result.rollback_detail = outcome.detail
            any_rolled = any_rolled or outcome.success
            log.info("rolled_back", incident=incident.incident_id,
                     action=result.action.type.value,
                     inverse=inverse.type.value, success=outcome.success,
                     detail=outcome.detail)
            await self._emit_action(result)
        return any_rolled

    # ── emission & persistence ───────────────────────────────────────
    async def _journal(self, incident: Incident, step: str, payload: dict) -> None:
        await self._memory.record_step(incident.incident_id, step, payload)

    async def _emit_decision(self, incident: Incident) -> None:
        from guardian_platform.topics import GUARDIAN_DECISIONS
        if incident.diagnosis:
            await self._emit(GUARDIAN_DECISIONS, incident.diagnosis, incident.incident_id)

    async def _emit_action(self, result: ActionResult) -> None:
        from guardian_platform.topics import GUARDIAN_ACTIONS
        await self._emit(GUARDIAN_ACTIONS, result, result.incident_id)

    async def _close(
        self, incident: Incident, results: list[ActionResult],
        verified: bool, detail: str, rolled_back: bool = False,
    ) -> None:
        from guardian_platform.topics import GUARDIAN_OUTCOMES

        diagnosis = incident.diagnosis
        outcome = Outcome(
            incident_id=incident.incident_id,
            service=incident.service,
            resolved=verified,
            root_cause=diagnosis.root_cause if diagnosis else "undiagnosed",
            actions_taken=[r.action.type.value for r in results if r.executed],
            mttr_seconds=incident.mttr_seconds or 0.0,
            verification_detail=detail,
            tokens_used=diagnosis.tokens_used if diagnosis else 0,
            cost_usd=diagnosis.cost_usd if diagnosis else 0.0,
            human_approved=any(
                r.decision.effect == "require_approval" and r.executed for r in results
            ),
            scenario=incident.scenario,
            autonomy_mode=self.autonomy.value,
            rolled_back=rolled_back,
        )
        await self._memory.close_incident(incident, outcome)
        await self._emit(GUARDIAN_OUTCOMES, outcome, incident.incident_id)
        self._failed_actions.pop(incident.incident_id, None)
        log.info("incident_closed", incident=incident.incident_id,
                 resolved=verified, mttr=round(outcome.mttr_seconds, 1),
                 root_cause=outcome.root_cause, detail=detail)

    def snapshot(self) -> dict:
        return {
            "open_incidents": [
                {"incident_id": i.incident_id, "service": i.service,
                 "state": i.state.value, "severity": i.severity.value,
                 "anomaly_count": len(i.anomalies)}
                for i in self._open.values()
            ],
            "awaiting_approval": list(self._pending),
            "budget": self._budget.snapshot(),
            "capabilities": self.capabilities.model_dump(),
            "kafka": self._kafka.describe(),
            "planner": self.planner_description,
            "autonomy": self.autonomy.value,
            "changes_tracked": self._changes.count if self._changes else 0,
        }
