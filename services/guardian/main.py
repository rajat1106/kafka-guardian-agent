"""Guardian agent service: Kafka wiring around the agent loop."""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys

import httpx
import uvicorn
from fastapi import FastAPI

sys.path.insert(0, "/app/platform")

from guardian_platform.config import (  # noqa: E402
    app_settings, guardian_settings, kafka_settings,
)
from guardian_platform.contracts import Anomaly, ApprovalResponse  # noqa: E402
from guardian_platform.kafka import EventConsumer, EventProducer, ensure_topics  # noqa: E402
from guardian_platform.obs import configure_logging  # noqa: E402
from guardian_platform.topics import GUARDIAN_ANOMALIES, GUARDIAN_APPROVALS  # noqa: E402

from actuators import build_actuator  # noqa: E402
from agent import GuardianAgent  # noqa: E402
from budget import TokenBudget  # noqa: E402
from memory import IncidentMemory  # noqa: E402
from planner_llm import LLMPlanner  # noqa: E402
from policy import PolicyGate  # noqa: E402
from runtime_config import BrainSupervisor  # noqa: E402

log = configure_logging("guardian")
FLEET_URL = os.getenv("FLEET_URL", "http://fleet:8081").rstrip("/")

app = FastAPI(title="Kafka Guardian Agent", version="2.0.0")
_agent: GuardianAgent | None = None
_brain: BrainSupervisor | None = None
_producer: EventProducer | None = None
_memory: IncidentMemory | None = None


async def _fleet_state(service: str) -> dict:
    """Read live service state. Used for planning context and verification."""
    try:
        async with httpx.AsyncClient(timeout=6) as c:
            r = await c.get(f"{FLEET_URL}/services")
            r.raise_for_status()
            return r.json().get(service, {})
    except Exception as exc:  # noqa: BLE001
        log.warning("fleet_state_unavailable", service=service, error=str(exc))
        return {}


async def _emit(spec, model, key: str | None = None) -> None:
    if _producer is not None:
        await _producer.send(spec, model, key=key)


async def _consume_anomalies() -> None:
    consumer = EventConsumer([GUARDIAN_ANOMALIES], "guardian-agent", Anomaly)
    await consumer.start()
    log.info("consuming_anomalies")
    try:
        async for anomaly in consumer:
            if _agent is not None:
                await _agent.on_anomaly(anomaly)
    finally:
        await consumer.stop()


async def _consume_approvals() -> None:
    # A distinct group id per process so every replica sees every approval;
    # approvals are addressed to whichever agent instance owns the incident.
    group = f"guardian-approvals-{os.getpid()}"
    consumer = EventConsumer([GUARDIAN_APPROVALS], group, ApprovalResponse)
    await consumer.start()
    log.info("consuming_approvals")
    try:
        async for response in consumer:
            if _agent is not None:
                await _agent.on_approval(response.incident_id, response.approved)
                log.info("approval_received", incident=response.incident_id,
                         approved=response.approved, by=response.approved_by)
    finally:
        await consumer.stop()


@app.get("/health")
async def health() -> dict:
    return {"ok": True, **(_agent.snapshot() if _agent else {})}


@app.get("/stats")
async def stats() -> dict:
    base = _agent.snapshot() if _agent else {}
    if _memory:
        base["memory"] = await _memory.stats()
    return base


@app.on_event("startup")
async def startup() -> None:
    global _agent, _producer, _memory

    ks, gs, aps = kafka_settings(), guardian_settings(), app_settings()
    log.info("guardian_starting", kafka=ks.describe(), actuator=aps.actuator_mode)

    await ensure_topics(ks)
    _producer = await EventProducer("guardian").start()

    _memory = IncidentMemory(aps.postgres_dsn)
    await _memory.connect()

    budget = TokenBudget(
        per_incident=gs.tokens_per_incident,
        per_hour=gs.tokens_per_hour,
        per_day=gs.tokens_per_day,
        min_severity=gs.llm_min_severity,
    )

    # An env key still works, but the Connections page is the primary path
    # and can swap the brain at runtime without a restart.
    api_key = aps.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY") or ""
    llm = LLMPlanner(api_key, gs, budget) if api_key.strip() else None
    if llm is None:
        log.info("offline_planner_only",
                 reason="no key configured yet — rules engine in use")

    actuator = build_actuator(aps.actuator_mode)
    log.info("actuator_ready", backends=actuator.describe())

    _agent = GuardianAgent(
        kafka=ks, guardian=gs, app=aps, memory=_memory,
        policy=PolicyGate(aps.opa_url, gs.auto_approve_max_blast),
        actuator=actuator, budget=budget, llm=llm,
        emit=_emit, fleet_state=_fleet_state,
    )

    unfinished = await _memory.unfinished_incidents()
    if unfinished:
        log.info("found_unfinished_incidents", count=len(unfinished),
                 note="their journaled steps will not be re-executed")

    def _swap_brain(planner, description: str) -> None:
        """Called by the supervisor whenever stored config changes."""
        if _agent is not None:
            _agent._llm = planner  # noqa: SLF001 — the supervisor owns this field
            _agent.planner_description = description

    global _brain
    _brain = BrainSupervisor(aps.postgres_dsn, gs, budget, _swap_brain)
    await _brain.start()

    app.state.tasks = [
        asyncio.create_task(_consume_anomalies()),
        asyncio.create_task(_consume_approvals()),
        asyncio.create_task(_brain.run()),
    ]


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in getattr(app.state, "tasks", []):
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if _producer:
        await _producer.stop()
    if _memory:
        await _memory.close()
    if _brain:
        await _brain.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083, log_config=None)
