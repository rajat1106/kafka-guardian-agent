"""Read model + WebSocket gateway for the dashboard.

Consumes every Guardian topic and folds them into an in-memory projection
the UI can render, then pushes deltas over a WebSocket. Deliberately a read
model: the only write path it exposes is the approval endpoint, which
publishes to `guardian.approvals` rather than calling the agent directly —
so a human approval travels the same audited path as everything else.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.insert(0, "/app/platform")

from guardian_platform.config import app_settings, kafka_settings  # noqa: E402
from guardian_platform.contracts import (  # noqa: E402
    ActionResult, Anomaly, ApprovalResponse, ServiceMetrics,
)
from guardian_platform.kafka import EventConsumer, EventProducer, ensure_topics  # noqa: E402
from guardian_platform.obs import configure_logging  # noqa: E402
from guardian_platform.topics import (  # noqa: E402
    GUARDIAN_ACTIONS, GUARDIAN_ANOMALIES, GUARDIAN_APPROVALS,
    GUARDIAN_DECISIONS, GUARDIAN_OUTCOMES, TELEMETRY_METRICS,
)

log = configure_logging("api")

FLEET_URL = os.getenv("FLEET_URL", "http://fleet:8081").rstrip("/")
CHAOS_URL = os.getenv("CHAOS_URL", "http://chaos:8082").rstrip("/")
GUARDIAN_URL = os.getenv("GUARDIAN_URL", "http://guardian:8083").rstrip("/")
# Slightly under the agent's own 300s approval timeout, so the card
# disappears just before clicking it would become a no-op.
APPROVAL_TTL_SECONDS = 285.0

# A fresh consumer-group id per process start. os.getpid() is not enough:
# inside a container the PID is always 1, so the group id is stable across
# restarts, Kafka finds committed offsets, and auto_offset_reset="earliest"
# is silently ignored — the replay never happens. Each API instance owns its
# own projection and should not share offsets with a previous one.
INSTANCE = uuid.uuid4().hex[:8]

app = FastAPI(title="Guardian API", version="2.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

_producer: EventProducer | None = None


def _age_seconds(iso: str | None) -> float:
    """Seconds since an ISO timestamp; treats a missing value as ancient."""
    if not iso:
        return float("inf")
    try:
        ts = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return float("inf")
    return (datetime.now(timezone.utc) - ts).total_seconds()


class Projection:
    """In-memory read model of everything the dashboard shows."""

    def __init__(self) -> None:
        self.metrics: dict[str, dict] = {}
        self.series: dict[str, Deque[dict]] = {}
        self.anomalies: Deque[dict] = deque(maxlen=100)
        self.decisions: Deque[dict] = deque(maxlen=60)
        self.actions: Deque[dict] = deque(maxlen=100)
        self.outcomes: Deque[dict] = deque(maxlen=60)
        # (payload, monotonic_deadline). Approvals expire: the agent stops
        # waiting after its own timeout, and a card that silently does
        # nothing when clicked is worse than no card at all.
        self.pending_approvals: dict[str, dict] = {}
        self._approval_deadlines: dict[str, float] = {}

    def add_metrics(self, m: ServiceMetrics) -> dict:
        payload = m.model_dump(mode="json")
        payload["db_pool_utilisation"] = round(m.db_pool_utilisation, 3)
        self.metrics[m.service] = payload
        series = self.series.setdefault(m.service, deque(maxlen=90))
        series.append({
            "ts": payload["ts"],
            "consumer_lag": m.consumer_lag,
            "memory_used_pct": m.memory_used_pct,
            "p99_latency_ms": m.p99_latency_ms,
            "error_rate": m.error_rate,
            "cpu_pct": m.cpu_pct,
            "db_pool_utilisation": round(m.db_pool_utilisation, 3),
        })
        return payload

    def add_approval(self, raw: dict) -> None:
        self.pending_approvals[raw['incident_id']] = raw
        self._approval_deadlines[raw['incident_id']] = (
            time.monotonic() + APPROVAL_TTL_SECONDS
        )

    def drop_approval(self, incident_id: str) -> None:
        self.pending_approvals.pop(incident_id, None)
        self._approval_deadlines.pop(incident_id, None)

    def expire_approvals(self) -> list[str]:
        now = time.monotonic()
        stale = [k for k, dl in self._approval_deadlines.items() if dl < now]
        for k in stale:
            self.drop_approval(k)
        return stale

    def snapshot(self) -> dict[str, Any]:
        return {
            "metrics": self.metrics,
            "series": {k: list(v) for k, v in self.series.items()},
            "anomalies": list(self.anomalies),
            "decisions": list(self.decisions),
            "actions": list(self.actions),
            "outcomes": list(self.outcomes),
            "pending_approvals": list(self.pending_approvals.values()),
        }


P = Projection()


class Hub:
    """WebSocket fan-out."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def join(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self._clients.add(ws)
        await ws.send_text(json.dumps({"type": "snapshot", "data": P.snapshot()}))

    async def leave(self, ws: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(ws)

    async def broadcast(self, kind: str, data: Any) -> None:
        if not self._clients:
            return
        message = json.dumps({"type": kind, "data": data})
        async with self._lock:
            targets = list(self._clients)
        dead = []
        for ws in targets:
            try:
                await ws.send_text(message)
            except Exception:  # noqa: BLE001
                dead.append(ws)
        if dead:
            async with self._lock:
                for ws in dead:
                    self._clients.discard(ws)


HUB = Hub()


# ── consumers ────────────────────────────────────────────────────────
async def _pump(spec, model, group: str, kind: str, handler,
                replay: bool = False) -> None:
    """Fold one topic into the projection.

    `replay` reads the topic from the beginning. The guardian topics are low
    volume with days of retention, and a read model that starts at `latest`
    shows an empty dashboard on every restart even though the history is
    sitting in Kafka. Replaying it is what makes the log the source of truth
    rather than the process's uptime. Telemetry stays at `latest` — nobody
    needs six hours of metric backfill to see a live chart.
    """
    consumer = EventConsumer(
        [spec], f"api-{group}-{INSTANCE}", model, from_beginning=replay
    )
    await consumer.start()
    try:
        async for event in consumer:
            payload = handler(event)
            if payload is not None:
                await HUB.broadcast(kind, payload)
    finally:
        await consumer.stop()


def _on_metrics(m: ServiceMetrics) -> dict:
    return P.add_metrics(m)


def _on_anomaly(a: Anomaly) -> dict:
    payload = a.model_dump(mode="json")
    P.anomalies.appendleft(payload)
    return payload


def _on_decision(raw: dict) -> dict:
    P.decisions.appendleft(raw)
    # An ApprovalRequest is distinguishable by carrying a plan and decisions.
    if "plan_id" in raw and "decisions" in raw:
        # Replay makes stale approval requests reappear. Anything older than
        # the agent's own timeout can no longer be answered, so surfacing it
        # would give the operator a button that does nothing.
        if _age_seconds(raw.get("requested_at")) < APPROVAL_TTL_SECONDS:
            P.add_approval(raw)
    return raw


def _on_action(r: ActionResult) -> dict:
    payload = r.model_dump(mode="json")
    P.actions.appendleft(payload)
    P.drop_approval(r.incident_id)
    return payload


def _on_outcome(raw: dict) -> dict:
    P.outcomes.appendleft(raw)
    P.drop_approval(raw.get("incident_id", ""))
    return raw


class _Passthrough(BaseModel):
    """Accepts any JSON object — decisions topic carries two shapes."""

    model_config = {"extra": "allow"}

    def model_dump(self, **kw):  # type: ignore[override]
        return super().model_dump(**kw)


async def _pump_raw(spec, group: str, kind: str, handler,
                    replay: bool = False) -> None:
    """For topics carrying more than one payload shape."""
    from aiokafka import AIOKafkaConsumer

    from guardian_platform.kafka import client_kwargs

    ks = kafka_settings()
    consumer = AIOKafkaConsumer(
        ks.topic(spec.name),
        group_id=f"api-{group}-{INSTANCE}",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest" if replay else "latest",
        **client_kwargs(ks),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            payload = handler(msg.value)
            if payload is not None:
                await HUB.broadcast(kind, payload)
    finally:
        await consumer.stop()


# ── HTTP ─────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health() -> dict:
    return {"ok": True, "kafka": kafka_settings().describe()}


@app.get("/api/state")
async def state() -> dict:
    return P.snapshot()


@app.get("/api/cluster")
async def cluster() -> dict:
    ks = kafka_settings()
    guardian: dict = {}
    with contextlib.suppress(Exception):
        async with httpx.AsyncClient(timeout=5) as c:
            guardian = (await c.get(f"{GUARDIAN_URL}/stats")).json()
    return {
        "provider": ks.provider.value,
        "description": ks.describe(),
        "bootstrap": ks.bootstrap_servers,
        "security_protocol": ks.security_protocol,
        "topic_prefix": ks.topic_prefix,
        "replication_factor": ks.replication_factor,
        "capabilities": ks.capabilities.model_dump(),
        "guardian": guardian,
    }



# ── lineage ──────────────────────────────────────────────────────────
# Business topic each demo service consumes from. The fleet simulates
# services, not topics, so the mapping lives here rather than being
# invented in the browser.
SERVICE_TOPICS = {
    "payment-service": "payment-events",
    "user-service": "user-events",
    "notification-service": "notification-events",
}

UPSTREAM_PRODUCERS = {
    "payment-events": ["checkout-api", "billing-worker"],
    "user-events": ["web-app", "mobile-app"],
    "notification-events": ["user-events-fanout"],
}


@app.get("/api/lineage")
async def lineage() -> dict:
    """Producer → topic → consumer-group graph with live metrics.

    Built from fleet state rather than a static fixture, so partition counts,
    replica counts and lag are whatever the cluster actually reports — and a
    node the agent has just acted on shows it.
    """
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            fleet = (await c.get(f"{FLEET_URL}/services")).json()
    except Exception as exc:  # noqa: BLE001
        return {"nodes": [], "edges": [], "error": str(exc)}

    ks = kafka_settings()
    nodes: list[dict] = []
    edges: list[dict] = []

    # Which services the agent has touched recently, so the graph can show it.
    recent_actions: dict[str, dict] = {}
    for a in list(P.actions)[:12]:
        target = a.get("action", {}).get("target")
        if target and target not in recent_actions:
            recent_actions[target] = {
                "type": a["action"]["type"],
                "success": a.get("success", False),
                "executed": a.get("executed", False),
                "blast_radius": a.get("decision", {}).get("blast_radius"),
                "ts": a.get("ts"),
            }

    # Services with a plan parked for human approval.
    awaiting: set[str] = set()
    for req in P.pending_approvals.values():
        for act in req.get("actions", []):
            target = act.get("target")
            if target:
                awaiting.add(target)

    for service, info in fleet.items():
        latest = info.get("latest") or {}
        topic = SERVICE_TOPICS.get(service, f"{service}-events")

        for producer in UPSTREAM_PRODUCERS.get(topic, []):
            pid = f"prod:{producer}"
            if not any(n["id"] == pid for n in nodes):
                nodes.append({
                    "id": pid, "kind": "producer", "label": producer,
                    "active": info.get("healthy", True),
                })
            edges.append({
                "id": f"{pid}->topic:{topic}", "source": pid, "target": f"topic:{topic}",
                "rate": round((latest.get("messages_per_sec") or 0)
                              / max(len(UPSTREAM_PRODUCERS.get(topic, [1])), 1), 1),
                "healthy": True,
            })

        nodes.append({
            "id": f"topic:{topic}", "kind": "topic", "label": ks.topic(topic),
            "partitions": info.get("partition_count", 0),
            "replication_factor": ks.replication_factor,
            "messages_per_sec": latest.get("messages_per_sec", 0),
            "under_replicated": latest.get("under_replicated_partitions", 0),
        })

        lag = latest.get("consumer_lag", 0)
        nodes.append({
            "id": f"group:{service}", "kind": "consumer", "label": service,
            "group_id": info.get("consumer_group"),
            "replicas": info.get("replicas", 0),
            "partitions": info.get("partition_count", 0),
            "lag": lag,
            "healthy": info.get("healthy", True),
            "memory_used_pct": latest.get("memory_used_pct", 0),
            "p99_latency_ms": latest.get("p99_latency_ms", 0),
            "error_rate": latest.get("error_rate", 0),
            "db_pool_used": latest.get("db_pool_used", 0),
            "db_pool_size": latest.get("db_pool_size", 1),
            "region": info.get("region", ""),
            "active_fault": info.get("active_fault"),
            "recent_action": recent_actions.get(service),
            "awaiting_approval": service in awaiting,
        })
        edges.append({
            "id": f"topic:{topic}->group:{service}",
            "source": f"topic:{topic}", "target": f"group:{service}",
            "rate": latest.get("messages_per_sec", 0),
            "lag": lag,
            "healthy": info.get("healthy", True) and lag < 500,
        })

    return {
        "nodes": nodes, "edges": edges,
        "cluster": ks.describe(), "provider": ks.provider.value,
    }


@app.get("/api/services")
async def services() -> dict:
    async with httpx.AsyncClient(timeout=8) as c:
        return (await c.get(f"{FLEET_URL}/services")).json()


@app.get("/api/scenarios")
async def scenarios() -> dict:
    async with httpx.AsyncClient(timeout=8) as c:
        return (await c.get(f"{CHAOS_URL}/scenarios")).json()


@app.post("/api/scenarios/{key}/inject")
async def inject(key: str) -> dict:
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{CHAOS_URL}/inject/{key}")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text)
        return r.json()


@app.post("/api/chaos/toggle")
async def chaos_toggle() -> dict:
    async with httpx.AsyncClient(timeout=8) as c:
        return (await c.post(f"{CHAOS_URL}/toggle")).json()


class ApprovalBody(BaseModel):
    incident_id: str
    plan_id: str = ""
    approved: bool
    approved_by: str = "operator"
    note: str = ""


@app.post("/api/approve")
async def approve(body: ApprovalBody) -> dict:
    """Publish a human decision onto the approvals topic."""
    if _producer is None:
        raise HTTPException(503, "producer not ready")
    was_pending = body.incident_id in P.pending_approvals
    response = ApprovalResponse(
        incident_id=body.incident_id, plan_id=body.plan_id,
        approved=body.approved, approved_by=body.approved_by, note=body.note,
    )
    await _producer.send(GUARDIAN_APPROVALS, response, key=body.incident_id)
    P.drop_approval(body.incident_id)
    await HUB.broadcast("approval", response.model_dump(mode="json"))
    log.info("approval_published", incident=body.incident_id, approved=body.approved,
             was_pending=was_pending)
    return {"published": True, "was_pending": was_pending,
            **response.model_dump(mode="json")}


@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    await HUB.join(websocket)
    try:
        while True:
            # The client does not send commands; this keeps the socket open
            # and detects disconnects.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await HUB.leave(websocket)


@app.on_event("startup")
async def startup() -> None:
    global _producer
    ks = kafka_settings()
    log.info("api_starting", kafka=ks.describe())
    await ensure_topics(ks)
    _producer = await EventProducer("api").start()
    app.state.tasks = [
        asyncio.create_task(_pump(TELEMETRY_METRICS, ServiceMetrics, "metrics",
                                  "metrics", _on_metrics)),
        asyncio.create_task(_pump(GUARDIAN_ANOMALIES, Anomaly, "anomalies",
                                  "anomaly", _on_anomaly, replay=True)),
        asyncio.create_task(_pump(GUARDIAN_ACTIONS, ActionResult, "actions",
                                  "action", _on_action, replay=True)),
        asyncio.create_task(_pump_raw(GUARDIAN_DECISIONS, "decisions",
                                      "decision", _on_decision, replay=True)),
        asyncio.create_task(_pump_raw(GUARDIAN_OUTCOMES, "outcomes",
                                      "outcome", _on_outcome, replay=True)),
        asyncio.create_task(_reap_approvals()),
    ]


async def _reap_approvals() -> None:
    while True:
        await asyncio.sleep(15)
        for incident_id in P.expire_approvals():
            log.info("approval_expired", incident=incident_id)
            await HUB.broadcast("approval_expired", {"incident_id": incident_id})


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in getattr(app.state, "tasks", []):
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if _producer:
        await _producer.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
