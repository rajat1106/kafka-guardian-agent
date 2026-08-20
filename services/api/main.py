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
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

sys.path.insert(0, "/app/platform")

from guardian_platform.config import app_settings, kafka_settings  # noqa: E402
from guardian_platform.contracts import (  # noqa: E402
    ActionResult, Anomaly, ApprovalResponse, ChangeEvent, ChangeKind,
    ServiceMetrics,
)
from guardian_platform.kafka import EventConsumer, EventProducer, ensure_topics  # noqa: E402
from guardian_platform.audit import AuditEvent, AuditLog  # noqa: E402
from guardian_platform.authz import (  # noqa: E402
    CAN_CONFIGURE, CAN_INJECT_CHAOS, CAN_ROLLBACK, Principal, issue_token,
)
from guardian_platform.obs import configure_logging  # noqa: E402
from guardian_platform.plugins import (  # noqa: E402
    SLOTS, SlotKind, catalogue, provider as get_provider,
)
from guardian_platform.topics import (  # noqa: E402
    GUARDIAN_ACTIONS, GUARDIAN_ANOMALIES, GUARDIAN_APPROVALS,
    GUARDIAN_DECISIONS, GUARDIAN_OUTCOMES, TELEMETRY_CHANGES,
    TELEMETRY_METRICS,
)

import connectors  # noqa: E402
import demo_control  # noqa: E402
from auth_dep import UserStore, current_principal, require, settings as auth_settings  # noqa: E402
from plugin_store import PluginStore  # noqa: E402

log = configure_logging("api")
_store: PluginStore | None = None
_users: UserStore | None = None
_audit: AuditLog | None = None

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
        self.changes: Deque[dict] = deque(maxlen=100)
        self.shadow: Deque[dict] = deque(maxlen=200)

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
            "changes": list(self.changes),
            "shadow": list(self.shadow),
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
    # The decisions topic carries diagnoses, approval requests and shadow
    # records; each is identified by the fields only it has.
    if "would_have_auto_executed" in raw:
        P.shadow.appendleft(raw)
        return raw
    P.decisions.appendleft(raw)
    # An ApprovalRequest is distinguishable by carrying a plan and decisions.
    if "plan_id" in raw and "decisions" in raw:
        # Replay makes stale approval requests reappear. Anything older than
        # the agent's own timeout can no longer be answered, so surfacing it
        # would give the operator a button that does nothing.
        if _age_seconds(raw.get("requested_at")) < APPROVAL_TTL_SECONDS:
            P.add_approval(raw)
    return raw


def _on_change(e: ChangeEvent) -> dict:
    payload = e.model_dump(mode="json")
    P.changes.appendleft(payload)
    return payload


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

# ── authentication ───────────────────────────────────────────────────

class LoginBody(BaseModel):
    username: str
    password: str


@app.post("/api/auth/login")
async def login(body: LoginBody) -> dict:
    s = auth_settings()
    if not s.enabled:
        raise HTTPException(400, "Authentication is disabled (AUTH_MODE=disabled)")
    if s.mode == "oidc":
        raise HTTPException(400, "This deployment uses OIDC; obtain a token from the IdP")
    if _users is None:
        raise HTTPException(503, "user store not ready")

    principal = await _users.authenticate(body.username, body.password)
    if principal is None:
        if _audit:
            await _audit.record(AuditEvent.LOGIN_FAILED, actor=body.username,
                                detail={"reason": "invalid credentials"})
        # Deliberately does not distinguish unknown user from wrong password.
        raise HTTPException(401, "Invalid username or password")

    token, expires = issue_token(principal, s.secret)
    if _audit:
        await _audit.record(AuditEvent.LOGIN, actor=principal.subject,
                            actor_name=principal.display_name,
                            actor_issuer=principal.issuer,
                            detail={"roles": [r.value for r in principal.roles]})
    return {"token": token, "expires_at": expires.isoformat(),
            "principal": principal.to_json()}


@app.get("/api/auth/me")
async def whoami(p: Principal = Depends(current_principal)) -> dict:
    return {"authenticated": auth_settings().enabled, "mode": auth_settings().mode,
            "principal": p.to_json()}


class CreateUserBody(BaseModel):
    username: str
    password: str
    display_name: str = ""
    email: str | None = None
    roles: list[str] = ["viewer"]


@app.get("/api/auth/users")
async def list_users(p: Principal = Depends(require(CAN_CONFIGURE))) -> dict:
    if _users is None:
        raise HTTPException(503, "user store not ready")
    return {"users": await _users.list_users()}


@app.post("/api/auth/users")
async def create_user(body: CreateUserBody,
                      p: Principal = Depends(require(CAN_CONFIGURE))) -> dict:
    from guardian_platform.authz import Role

    if _users is None:
        raise HTTPException(503, "user store not ready")
    if len(body.password) < 12:
        raise HTTPException(400, "Password must be at least 12 characters")
    try:
        roles = [Role(r.lower()) for r in body.roles]
    except ValueError as exc:
        raise HTTPException(400, f"Unknown role: {exc}") from None

    subject = await _users.create(
        username=body.username, password=body.password,
        display_name=body.display_name or body.username,
        roles=roles, email=body.email,
    )
    if _audit:
        await _audit.record(
            AuditEvent.PLUGIN_CONFIGURED, actor=p.subject,
            actor_name=p.display_name, actor_issuer=p.issuer, subject=subject,
            detail={"created_user": body.username,
                    "roles": [r.value for r in roles]},
        )
    return {"created": True, "subject": subject}


@app.get("/api/auth/config")
async def auth_config() -> dict:
    """What the login screen needs to know before anyone has a token."""
    s = auth_settings()
    return {"mode": s.mode, "enabled": s.enabled,
            "oidc_issuer": s.oidc_issuer if s.mode == "oidc" else None}


# ── audit ────────────────────────────────────────────────────────────

@app.get("/api/audit")
async def audit_recent(limit: int = 100, event: str | None = None,
                       subject: str | None = None,
                       p: Principal = Depends(current_principal)) -> dict:
    if _audit is None:
        raise HTTPException(503, "audit log not ready")
    return {"records": await _audit.recent(limit=limit, event=event, subject=subject),
            "head": await _audit.head()}


@app.get("/api/audit/verify")
async def audit_verify(p: Principal = Depends(current_principal)) -> dict:
    """Walk the hash chain and report the first break, if any."""
    if _audit is None:
        raise HTTPException(503, "audit log not ready")
    return await _audit.verify()


@app.get("/api/health")
async def health() -> dict:
    return {"ok": True, "kafka": kafka_settings().describe()}


@app.get("/api/state")
async def state() -> dict:
    return P.snapshot()



@app.get("/api/incidents")
async def incidents() -> dict:
    """Closed incidents, newest first — the list the UI paginates."""
    return {"incidents": list(P.outcomes)}


@app.get("/api/incidents/{incident_id}")
async def incident_detail(incident_id: str) -> dict:
    """Everything that happened in one incident, assembled into a timeline.

    Correlating four topics in the browser would mean shipping all of them to
    every client and re-deriving the story on each render. The projection
    already holds them here, keyed by incident, so the join belongs here.
    """
    anomalies = [a for a in P.anomalies if a.get("service")]
    actions = [a for a in P.actions if a.get("incident_id") == incident_id]
    outcome = next((o for o in P.outcomes if o.get("incident_id") == incident_id), None)
    diagnosis = next(
        (d for d in P.decisions
         if d.get("incident_id") == incident_id and "root_cause" in d),
        None,
    )
    approval = next(
        (d for d in P.decisions
         if d.get("incident_id") == incident_id and "decisions" in d),
        None,
    )

    service = (outcome or diagnosis or {}).get("service")
    if not service and actions:
        service = actions[0].get("action", {}).get("target")

    # Anomalies carry no incident id — they are what *opened* the incident.
    # Match on service within a window around the incident, which is how the
    # agent correlated them in the first place.
    window: list[dict] = []
    if service and outcome:
        try:
            end = datetime.fromisoformat(outcome["ts"].replace("Z", "+00:00"))
            span = float(outcome.get("mttr_seconds") or 0) + 30
            start = end.timestamp() - span
            window = [
                a for a in anomalies
                if a.get("service") == service
                and start <= datetime.fromisoformat(
                    a["ts"].replace("Z", "+00:00")).timestamp() <= end.timestamp()
            ]
        except (ValueError, KeyError, TypeError):
            window = [a for a in anomalies if a.get("service") == service][:6]

    # A flat, ordered list is what a timeline component wants; building it here
    # keeps ordering rules in one place instead of in every consumer.
    timeline: list[dict] = []
    for a in reversed(window):
        timeline.append({"kind": "detected", "ts": a["ts"], "payload": a})
    if diagnosis:
        timeline.append({"kind": "diagnosed", "ts": diagnosis["ts"],
                         "payload": diagnosis})
    if approval:
        timeline.append({"kind": "held_for_approval",
                         "ts": approval.get("requested_at", ""), "payload": approval})
    for r in sorted(actions, key=lambda x: x.get("ts", "")):
        timeline.append({"kind": "acted", "ts": r["ts"], "payload": r})
    if outcome:
        timeline.append({"kind": "closed", "ts": outcome["ts"], "payload": outcome})

    return {
        "incident_id": incident_id,
        "service": service,
        "outcome": outcome,
        "diagnosis": diagnosis,
        "approval": approval,
        "actions": sorted(actions, key=lambda x: x.get("ts", "")),
        "anomalies": window,
        "timeline": timeline,
    }



# ── change feed ──────────────────────────────────────────────────────

class ChangeBody(BaseModel):
    """What a CI/CD pipeline posts when it ships something."""

    kind: str = "deploy"
    service: str
    summary: str
    reference: str = ""
    author: str = ""
    version: str = ""
    metadata: dict[str, Any] = {}


@app.post("/api/changes")
async def post_change(body: ChangeBody,
                      p: Principal = Depends(current_principal)) -> dict:
    """Record a deploy, config change or scaling event.

    Point your pipeline at this after a successful deploy. The agent keeps a
    30-minute window and offers anything in it as a candidate cause, which is
    the difference between "lag is high" and "lag rose 90 seconds after
    deploy a3f2c1".
    """
    if _producer is None:
        raise HTTPException(503, "producer not ready")
    try:
        kind = ChangeKind(body.kind.lower())
    except ValueError:
        raise HTTPException(
            400, f"kind must be one of: {', '.join(k.value for k in ChangeKind)}"
        ) from None

    event = ChangeEvent(
        kind=kind, service=body.service, summary=body.summary,
        reference=body.reference, author=body.author or p.subject,
        version=body.version, source="api", metadata=body.metadata,
    )
    await _producer.send(TELEMETRY_CHANGES, event, key=body.service)
    P.changes.appendleft(event.model_dump(mode="json"))
    await HUB.broadcast("change", event.model_dump(mode="json"))
    log.info("change_recorded", kind=kind.value, service=body.service,
             reference=body.reference, by=p.subject)
    return {"recorded": True, "change_id": event.change_id}


@app.get("/api/changes")
async def list_changes() -> dict:
    return {"changes": list(P.changes)}


# ── autonomy ─────────────────────────────────────────────────────────

@app.get("/api/autonomy")
async def get_autonomy() -> dict:
    guardian: dict = {}
    with contextlib.suppress(Exception):
        async with httpx.AsyncClient(timeout=5) as c:
            guardian = (await c.get(f"{GUARDIAN_URL}/health")).json()
    return {
        "mode": guardian.get("autonomy", "unknown"),
        "modes": [
            {"value": "shadow", "label": "Shadow",
             "description": "Decide everything, execute nothing. Builds a "
                            "record of what the agent would have done."},
            {"value": "supervised", "label": "Supervised",
             "description": "Execute low-blast-radius actions; everything "
                            "wider waits for a human."},
            {"value": "autonomous", "label": "Autonomous",
             "description": "As supervised, with a wider unattended ceiling."},
        ],
    }


@app.get("/api/shadow/report")
async def shadow_report() -> dict:
    """What the agent would have done, and how much of it needed nobody.

    The number that matters when deciding whether to leave shadow mode.
    """
    records = list(P.shadow)
    auto = [r for r in records if r.get("would_have_auto_executed")]
    by_cause: dict[str, int] = {}
    for r in records:
        by_cause[r.get("root_cause", "unknown")] = by_cause.get(
            r.get("root_cause", "unknown"), 0) + 1
    return {
        "total": len(records),
        "would_have_auto_executed": len(auto),
        "would_have_needed_a_human": len(records) - len(auto),
        "automation_rate": round(len(auto) / len(records), 3) if records else 0.0,
        "by_root_cause": by_cause,
        "records": records[:60],
    }



@app.get("/api/approvals/{incident_id}/context")
async def approval_context(incident_id: str) -> dict:
    """Everything a person needs to answer "should I approve this?".

    The card previously showed blast radius and the policy's reasoning, which
    says what the action *is* but nothing about the decision. The questions
    someone actually has at 3am are: what happens if I do nothing, how long do
    I have, what did we do last time, and how often is the agent right about
    this. All four are derivable from data already held here.
    """
    request = P.pending_approvals.get(incident_id)
    diagnosis = next(
        (d for d in P.decisions
         if d.get("incident_id") == incident_id and "root_cause" in d),
        None,
    )
    cause = (diagnosis or {}).get("root_cause")

    # Track record: how often this diagnosis led to a resolved incident.
    same_cause = [o for o in P.outcomes if o.get("root_cause") == cause]
    resolved = [o for o in same_cause if o.get("resolved")]
    prior = [
        {
            "incident_id": o.get("incident_id"),
            "ts": o.get("ts"),
            "resolved": o.get("resolved"),
            "actions_taken": o.get("actions_taken", []),
            "mttr_seconds": o.get("mttr_seconds"),
            "verification_detail": o.get("verification_detail", ""),
            "human_approved": o.get("human_approved", False),
        }
        for o in same_cause[:5]
    ]

    # Time remaining, straight from the detector's own projection.
    # Diagnosis carries no service field — it is keyed by incident — so the
    # service comes from the action's target or the closing outcome.
    service = None
    if request and request.get("actions"):
        service = request["actions"][0].get("target")
    if not service:
        service = next(
            (o.get("service") for o in P.outcomes
             if o.get("incident_id") == incident_id), None,
        )
    if not service:
        service = next(
            (a["action"].get("target") for a in P.actions
             if a.get("incident_id") == incident_id and a.get("action")), None,
        )
    breaches = [
        a for a in P.anomalies
        if a.get("service") == service and a.get("predicted_breach_seconds")
    ]
    soonest = min((a["predicted_breach_seconds"] for a in breaches), default=None)
    breach_metric = next(
        (a["metric"] for a in breaches
         if a["predicted_breach_seconds"] == soonest), None
    ) if soonest is not None else None

    # What the agent does if this is rejected: it does not retry, it escalates.
    action_type = (request["actions"][0]["type"]
                   if request and request.get("actions") else None)
    inverse = (request["actions"][0].get("inverse")
               if request and request.get("actions") else None)

    return {
        "incident_id": incident_id,
        "service": service,
        "root_cause": cause,
        "confidence": (diagnosis or {}).get("confidence"),
        "reasoning": (diagnosis or {}).get("reasoning", ""),
        "correlated_changes": (diagnosis or {}).get("correlated_changes", []),
        "action_type": action_type,
        "undo": inverse,
        "time": {
            "seconds_until_breach": soonest,
            "metric": breach_metric,
        },
        "track_record": {
            "seen": len(same_cause),
            "resolved": len(resolved),
            "rate": round(len(resolved) / len(same_cause), 2) if same_cause else None,
        },
        "prior_incidents": prior,
        "if_rejected": (
            "The agent takes no action and the incident is escalated for a "
            "person to handle. It does not try something smaller on its own."
        ),
    }


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



# ── plugins ──────────────────────────────────────────────────────────

def _slot(name: str) -> SlotKind:
    try:
        return SlotKind(name)
    except ValueError:
        raise HTTPException(404, f"unknown slot {name!r}") from None


@app.get("/api/plugins")
async def plugins() -> dict:
    """Catalogue plus current state. Secrets are masked by the store."""
    configured = await _store.public_view() if _store else []
    return {
        "catalogue": catalogue(),
        "configured": configured,
        "demo": await demo_control.status(),
    }


class PluginConfigBody(BaseModel):
    provider_id: str
    config: dict[str, Any] = {}


@app.post("/api/plugins/{slot}/configure")
async def configure_plugin(slot: str, body: PluginConfigBody,
                           p: Principal = Depends(require(CAN_CONFIGURE))) -> dict:
    """Persist configuration. Secrets go in and never come back out."""
    if _store is None:
        raise HTTPException(503, "plugin store not ready")
    slot_enum = _slot(slot)
    if get_provider(slot_enum, body.provider_id) is None:
        raise HTTPException(400, f"unknown provider {body.provider_id!r} for {slot}")
    await _store.save(slot_enum, body.provider_id, body.config)
    if _audit:
        # Field names only — never values. An audit record that quotes the
        # secret it was protecting defeats the purpose.
        await _audit.record(
            AuditEvent.PLUGIN_CONFIGURED, actor=p.subject,
            actor_name=p.display_name, actor_issuer=p.issuer, subject=slot,
            detail={"provider": body.provider_id,
                    "fields_set": sorted(body.config.keys())},
        )
    log.info("plugin_configured", slot=slot, provider=body.provider_id,
             by=p.subject, fields=sorted(body.config.keys()))
    return {"saved": True, "plugins": await _store.public_view()}


@app.post("/api/plugins/{slot}/test")
async def test_plugin(slot: str,
                      p: Principal = Depends(require(CAN_CONFIGURE))) -> dict:
    """Run a real connection test against the saved configuration."""
    if _store is None:
        raise HTTPException(503, "plugin store not ready")
    slot_enum = _slot(slot)
    entry = await _store.raw(slot_enum)
    if entry is None:
        raise HTTPException(404, f"{slot} is not configured")

    tester = connectors.TESTERS[slot_enum]
    result = await tester(entry["provider_id"], entry["config"])
    ok = bool(result.pop("ok", False))
    await _store.record_test(slot_enum, ok, result)
    if _audit:
        await _audit.record(AuditEvent.PLUGIN_TESTED, actor=p.subject,
                            actor_name=p.display_name, actor_issuer=p.issuer,
                            subject=slot,
                            detail={"provider": entry["provider_id"], "ok": ok})
    log.info("plugin_tested", slot=slot, provider=entry["provider_id"],
             ok=ok, by=p.subject)
    return {"ok": ok, **result, "plugins": await _store.public_view()}


@app.get("/api/plugins/source/discover")
async def discover_source() -> dict:
    """Fetch topics, partitions and consumer groups from the live cluster."""
    if _store is None:
        raise HTTPException(503, "plugin store not ready")
    entry = await _store.raw(SlotKind.SOURCE)
    if entry is None:
        raise HTTPException(404, "no source configured")
    return await connectors.discover_kafka(entry["provider_id"], entry["config"])


# ── demo cluster lifecycle ───────────────────────────────────────────

@app.get("/api/demo/status")
async def demo_status() -> dict:
    return await demo_control.status()


@app.post("/api/demo/start")
async def demo_start() -> dict:
    return await demo_control.start_cluster()


@app.post("/api/demo/recover")
async def demo_recover() -> dict:
    """One-click undo for anything chaos did to the demo cluster."""
    return await demo_control.recover_cluster()


@app.post("/api/demo/stop")
async def demo_stop() -> dict:
    return await demo_control.stop_cluster()


@app.get("/api/demo/scenarios")
async def demo_scenarios() -> dict:
    """Simulated faults plus the container-level ones."""
    simulated: list = []
    with contextlib.suppress(Exception):
        async with httpx.AsyncClient(timeout=8) as c:
            simulated = (await c.get(f"{CHAOS_URL}/scenarios")).json()["scenarios"]
    return {
        "simulated": simulated,
        "infrastructure": [
            {"key": k, **v} for k, v in demo_control.INFRA_SCENARIOS.items()
        ],
    }


@app.post("/api/demo/infra/{key}/inject")
async def demo_infra_inject(
    key: str, p: Principal = Depends(require(CAN_INJECT_CHAOS))
) -> dict:
    result = await demo_control.inject_infra(key)
    if _audit:
        await _audit.record(AuditEvent.CHAOS_INJECTED, actor=p.subject,
                            actor_name=p.display_name, actor_issuer=p.issuer,
                            subject=key, detail={"kind": "infrastructure",
                                                 "ok": result.get("ok")})
    return result


@app.post("/api/demo/infra/{key}/recover")
async def demo_infra_recover(key: str) -> dict:
    return await demo_control.recover_infra(key)


@app.get("/api/services")
async def services() -> dict:
    async with httpx.AsyncClient(timeout=8) as c:
        return (await c.get(f"{FLEET_URL}/services")).json()


@app.get("/api/scenarios")
async def scenarios() -> dict:
    async with httpx.AsyncClient(timeout=8) as c:
        return (await c.get(f"{CHAOS_URL}/scenarios")).json()


@app.post("/api/scenarios/{key}/inject")
async def inject(key: str,
                 p: Principal = Depends(require(CAN_INJECT_CHAOS))) -> dict:
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{CHAOS_URL}/inject/{key}")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text)
        if _audit:
            await _audit.record(AuditEvent.CHAOS_INJECTED, actor=p.subject,
                                actor_name=p.display_name,
                                actor_issuer=p.issuer, subject=key,
                                detail={"kind": "simulated"})
        return r.json()


@app.post("/api/chaos/toggle")
async def chaos_toggle() -> dict:
    async with httpx.AsyncClient(timeout=8) as c:
        return (await c.post(f"{CHAOS_URL}/toggle")).json()


class ApprovalBody(BaseModel):
    incident_id: str
    plan_id: str = ""
    approved: bool
    note: str = ""
    # `approved_by` is deliberately absent. It used to be accepted here, which
    # meant the audit record said whatever the client typed. Identity now comes
    # from the caller's token and nowhere else.


@app.post("/api/approve")
async def approve(body: ApprovalBody,
                  p: Principal = Depends(current_principal)) -> dict:
    """Publish a human decision onto the approvals topic."""
    if _producer is None:
        raise HTTPException(503, "producer not ready")
    was_pending = body.incident_id in P.pending_approvals

    # Authorisation is measured on the same 0-5 scale as the remediation
    # policy: an operator may approve a service restart, only an approver may
    # sign off on a region failover.
    pending = P.pending_approvals.get(body.incident_id)
    blast = 0
    if pending and pending.get("decisions"):
        blast = int(pending["decisions"][0].get("blast_radius", 0))
    if body.approved and auth_settings().enabled and not p.may_approve(blast):
        if _audit:
            await _audit.record(
                AuditEvent.APPROVAL_FORBIDDEN, actor=p.subject,
                actor_name=p.display_name, actor_issuer=p.issuer,
                subject=body.incident_id,
                detail={"blast_radius": blast, "max_allowed": p.max_blast,
                        "roles": [r.value for r in p.roles]},
            )
        raise HTTPException(
            403,
            f"This action has blast radius {blast}; your role allows up to "
            f"{p.max_blast}. Escalate to someone with a wider remit.",
        )

    response = ApprovalResponse(
        incident_id=body.incident_id, plan_id=body.plan_id,
        approved=body.approved, approved_by=p.subject, note=body.note,
    )
    if _audit:
        await _audit.record(
            AuditEvent.APPROVAL_GRANTED if body.approved
            else AuditEvent.APPROVAL_DENIED,
            actor=p.subject, actor_name=p.display_name, actor_issuer=p.issuer,
            subject=body.incident_id,
            detail={"plan_id": body.plan_id, "blast_radius": blast,
                    "note": body.note, "was_pending": was_pending},
        )
    await _producer.send(GUARDIAN_APPROVALS, response, key=body.incident_id)
    P.drop_approval(body.incident_id)
    await HUB.broadcast("approval", response.model_dump(mode="json"))
    log.info("approval_published", incident=body.incident_id,
             approved=body.approved, by=p.subject, was_pending=was_pending)
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

    global _store, _users, _audit
    dsn = app_settings().postgres_dsn

    problems = auth_settings().validate()
    if problems:
        # Refusing to start beats starting with authentication silently off.
        raise RuntimeError("auth configuration invalid: " + "; ".join(problems))

    _store = PluginStore(dsn)
    await _store.connect()
    _audit = AuditLog(dsn)
    await _audit.connect()
    _users = UserStore(dsn)
    await _users.connect()

    if auth_settings().enabled:
        log.info("auth_enabled", mode=auth_settings().mode)
    else:
        log.warning("auth_disabled",
                    note="every endpoint is unauthenticated — set AUTH_MODE "
                         "before exposing this beyond localhost")

    app.state.tasks = [
        asyncio.create_task(_pump(TELEMETRY_METRICS, ServiceMetrics, "metrics",
                                  "metrics", _on_metrics)),
        asyncio.create_task(_pump(GUARDIAN_ANOMALIES, Anomaly, "anomalies",
                                  "anomaly", _on_anomaly, replay=True)),
        asyncio.create_task(_pump(TELEMETRY_CHANGES, ChangeEvent, "changes",
                                  "change", _on_change, replay=True)),
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
    if _store:
        await _store.close()
    if _users:
        await _users.close()
    if _audit:
        await _audit.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
