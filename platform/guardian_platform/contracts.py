"""Event contracts.

Every message on every topic is one of these models.  They are the API
between the five services, so they are deliberately explicit: an
`Anomaly` says what was measured and why it was strange, a `Diagnosis`
says what the agent believes and how sure it is, and a `RemediationPlan`
carries typed actions rather than a shell string.

Typed actions matter more than they look.  The moment a plan is
"run this command", the policy engine can no longer reason about it and
the blast radius becomes unknowable.  Every mutating capability the agent
has is an `ActionType` below, and that closed set is what the Rego policy
is written against.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class ServiceMetrics(BaseModel):
    """One observation window from one service."""

    event_id: str = Field(default_factory=lambda: _id("met"))
    ts: datetime = Field(default_factory=_now)
    service: str
    # Kafka-side signals
    consumer_lag: int = 0
    messages_per_sec: float = 0.0
    partition_count: int = 0
    under_replicated_partitions: int = 0
    # Host-side signals
    memory_used_pct: float = 0.0
    cpu_pct: float = 0.0
    heap_used_mb: float = 0.0
    # Application-side signals
    db_pool_used: int = 0
    db_pool_size: int = 1
    p99_latency_ms: float = 0.0
    error_rate: float = 0.0
    request_rate: float = 0.0
    healthy: bool = True
    region: str = "us-east-1"

    @property
    def db_pool_utilisation(self) -> float:
        return self.db_pool_used / max(self.db_pool_size, 1)


class Anomaly(BaseModel):
    """The detector's output. No LLM has seen this yet."""

    anomaly_id: str = Field(default_factory=lambda: _id("anom"))
    ts: datetime = Field(default_factory=_now)
    service: str
    metric: str
    value: float
    baseline: float
    deviation_sigma: float
    severity: Severity
    # 0..1 — how confident the detector is that this is real, used to
    # decide whether the incident is worth spending tokens on.
    score: float
    detector: Literal["zscore", "isolation_forest", "trend_forecast", "threshold"]
    # For trend_forecast: seconds until the metric crosses its threshold.
    predicted_breach_seconds: float | None = None
    description: str = ""
    window: list[float] = Field(default_factory=list)


class ActionType(str, Enum):
    """The complete set of things the agent can do to the world.

    Closed by design — the Rego policy in policy/guardian.rego assigns a
    blast-radius score to each member, and an action type with no policy
    entry is denied rather than defaulted.
    """

    SCALE_CONSUMER_GROUP = "scale_consumer_group"
    INCREASE_PARTITIONS = "increase_partitions"
    RESET_CONSUMER_OFFSET = "reset_consumer_offset"
    THROTTLE_PRODUCER = "throttle_producer"
    ADJUST_DB_POOL = "adjust_db_pool"
    RESTART_SERVICE = "restart_service"
    CLEAR_SERVICE_BACKLOG = "clear_service_backlog"
    ROLL_BROKER = "roll_broker"
    FAILOVER_REGION = "failover_region"
    ALTER_TOPIC_CONFIG = "alter_topic_config"
    NO_OP = "no_op"


class Action(BaseModel):
    action_id: str = Field(default_factory=lambda: _id("act"))
    type: ActionType
    target: str
    params: dict[str, Any] = Field(default_factory=dict)
    rationale: str = ""
    reversible: bool = True
    # Filled by the policy engine, not the planner.
    blast_radius: int | None = None


class PolicyDecision(BaseModel):
    """Result of evaluating one action against the Rego policy."""

    effect: Literal["allow", "require_approval", "deny"]
    blast_radius: int
    reasons: list[str] = Field(default_factory=list)
    matched_rule: str = ""


class Diagnosis(BaseModel):
    """What the agent believes is happening, and why."""

    diagnosis_id: str = Field(default_factory=lambda: _id("diag"))
    ts: datetime = Field(default_factory=_now)
    incident_id: str
    root_cause: str
    confidence: float
    reasoning: str
    evidence: list[str] = Field(default_factory=list)
    similar_incidents: list[str] = Field(default_factory=list)
    # "llm" when Claude produced it, "offline" for the deterministic planner.
    source: Literal["llm", "offline"] = "offline"
    model: str | None = None
    tokens_used: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0


class RemediationPlan(BaseModel):
    plan_id: str = Field(default_factory=lambda: _id("plan"))
    incident_id: str
    actions: list[Action] = Field(default_factory=list)
    expected_outcome: str = ""
    verification_metric: str | None = None
    verification_window_seconds: int = 60


class IncidentState(str, Enum):
    DETECTED = "detected"
    CORRELATING = "correlating"
    DIAGNOSING = "diagnosing"
    PLANNING = "planning"
    AWAITING_APPROVAL = "awaiting_approval"
    EXECUTING = "executing"
    VERIFYING = "verifying"
    RESOLVED = "resolved"
    FAILED = "failed"
    ESCALATED = "escalated"


class Incident(BaseModel):
    incident_id: str = Field(default_factory=lambda: _id("inc"))
    opened_at: datetime = Field(default_factory=_now)
    closed_at: datetime | None = None
    state: IncidentState = IncidentState.DETECTED
    service: str
    title: str = ""
    severity: Severity = Severity.WARNING
    anomalies: list[Anomaly] = Field(default_factory=list)
    diagnosis: Diagnosis | None = None
    plan: RemediationPlan | None = None
    scenario: str | None = None

    @property
    def mttr_seconds(self) -> float | None:
        if self.closed_at is None:
            return None
        return (self.closed_at - self.opened_at).total_seconds()


class ActionResult(BaseModel):
    result_id: str = Field(default_factory=lambda: _id("res"))
    ts: datetime = Field(default_factory=_now)
    incident_id: str
    action: Action
    decision: PolicyDecision
    executed: bool
    success: bool = False
    detail: str = ""
    actuator: str = "simulated"
    duration_ms: int = 0


class ApprovalRequest(BaseModel):
    incident_id: str
    plan_id: str
    actions: list[Action]
    decisions: list[PolicyDecision]
    requested_at: datetime = Field(default_factory=_now)
    expires_at: datetime | None = None


class ApprovalResponse(BaseModel):
    incident_id: str
    plan_id: str
    approved: bool
    approved_by: str = "operator"
    note: str = ""
    ts: datetime = Field(default_factory=_now)


class Outcome(BaseModel):
    """Closing record for an incident — the agent's training signal.

    Written to `guardian.outcomes` and to the Postgres incident memory,
    so the next similar incident can be matched against it.
    """

    outcome_id: str = Field(default_factory=lambda: _id("out"))
    ts: datetime = Field(default_factory=_now)
    incident_id: str
    service: str
    resolved: bool
    root_cause: str
    actions_taken: list[str] = Field(default_factory=list)
    mttr_seconds: float = 0.0
    verification_detail: str = ""
    tokens_used: int = 0
    cost_usd: float = 0.0
    human_approved: bool = False
    scenario: str | None = None
