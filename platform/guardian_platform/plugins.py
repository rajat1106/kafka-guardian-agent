"""Plugin registry.

The system has four extension points, and each is a slot that accepts one
provider at a time:

    source   where Kafka telemetry comes from      (demo | self-hosted | confluent)
    brain    what makes the diagnosis              (offline | anthropic)
    notify   where incident notices are sent       (none | webhook | slack)
    monitor  extra signals fed to the detector     (none | prometheus)

A slot is described by its provider's `fields`, which drives both the form the
UI renders and the validation the API applies — so adding a provider is a
change here, not a change in three places.

Secrets are marked `secret: True`. Those values are accepted from the browser,
stored server-side, and never sent back: reads return a masked form
("••••4f21"). The UI is therefore able to show *that* a credential is set and
*which* one, without ever holding it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal


class SlotKind(str, Enum):
    SOURCE = "source"
    BRAIN = "brain"
    NOTIFY = "notify"
    MONITOR = "monitor"


class Status(str, Enum):
    UNCONFIGURED = "unconfigured"
    TESTING = "testing"
    CONNECTED = "connected"
    ERROR = "error"


FieldType = Literal["text", "password", "select", "number", "boolean", "textarea"]


@dataclass(frozen=True)
class Field:
    name: str
    label: str
    type: FieldType = "text"
    required: bool = True
    secret: bool = False
    placeholder: str = ""
    help: str = ""
    default: Any = None
    options: list[dict[str, str]] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name, "label": self.label, "type": self.type,
            "required": self.required, "secret": self.secret,
            "placeholder": self.placeholder, "help": self.help,
            "default": self.default, "options": self.options,
        }


@dataclass(frozen=True)
class Provider:
    id: str
    slot: SlotKind
    name: str
    summary: str
    fields: list[Field] = field(default_factory=list)
    # Shown in the UI before the user commits to a provider.
    notes: str = ""
    # A provider that needs no configuration at all (offline brain, no-op notify).
    zero_config: bool = False
    recommended: bool = False

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id, "slot": self.slot.value, "name": self.name,
            "summary": self.summary, "notes": self.notes,
            "zero_config": self.zero_config, "recommended": self.recommended,
            "fields": [f.to_json() for f in self.fields],
        }

    def secret_fields(self) -> set[str]:
        return {f.name for f in self.fields if f.secret}


# ── source: where Kafka telemetry comes from ─────────────────────────

DEMO_SOURCE = Provider(
    id="demo",
    slot=SlotKind.SOURCE,
    name="Demo cluster",
    summary="A real Kafka broker in Docker, with simulated services to break.",
    notes=(
        "Starts an actual Apache Kafka 4.x broker in KRaft mode alongside three "
        "simulated microservices. Nothing is mocked — topics, partitions, "
        "consumer groups and lag are real. Use this to watch the agent work "
        "before pointing it at anything you care about."
    ),
    zero_config=True,
    recommended=True,
)

SELF_HOSTED_SOURCE = Provider(
    id="self-hosted",
    slot=SlotKind.SOURCE,
    name="Self-hosted Kafka",
    summary="Your own Kafka 4.x cluster, with or without SASL.",
    notes=(
        "Full administrative access is assumed, so broker-level remedies are "
        "available to the agent."
    ),
    fields=[
        Field("bootstrap_servers", "Bootstrap servers",
              placeholder="broker-1:9092,broker-2:9092",
              help="Comma-separated host:port list."),
        Field("security_protocol", "Security protocol", type="select",
              default="PLAINTEXT",
              options=[{"value": v, "label": v} for v in
                       ("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL", "SSL")]),
        Field("sasl_mechanism", "SASL mechanism", type="select", required=False,
              default="PLAIN",
              options=[{"value": v, "label": v} for v in
                       ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")],
              help="Only used when the protocol includes SASL."),
        Field("sasl_username", "Username", required=False),
        Field("sasl_password", "Password", type="password", required=False, secret=True),
        Field("topic_prefix", "Topic prefix", required=False,
              placeholder="kga-dev.",
              help="Namespaces the agent's own topics on a shared cluster."),
        Field("replication_factor", "Replication factor", type="number", default=1,
              required=False),
    ],
)

CONFLUENT_SOURCE = Provider(
    id="confluent",
    slot=SlotKind.SOURCE,
    name="Confluent Cloud",
    summary="A managed Confluent cluster, via a cluster API key.",
    notes=(
        "Confluent Cloud is fully managed, so brokers are not yours: the agent "
        "will not propose broker restarts and escalates those cases to a human "
        "instead. Security protocol, SASL mechanism and the minimum replication "
        "factor of 3 are applied automatically."
    ),
    fields=[
        Field("bootstrap_servers", "Bootstrap server",
              placeholder="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
              help="Found under Cluster settings → Endpoints."),
        Field("sasl_username", "Cluster API key",
              placeholder="ABCDEFGH12345678",
              help="Create under API keys. A cluster key, not a Cloud key."),
        Field("sasl_password", "Cluster API secret", type="password", secret=True),
        Field("topic_prefix", "Topic prefix", required=False,
              placeholder="kga-dev.",
              help="Recommended on a shared cluster so the agent's topics do "
                   "not collide with other teams'."),
    ],
)

# ── brain: what makes the diagnosis ──────────────────────────────────

OFFLINE_BRAIN = Provider(
    id="offline",
    slot=SlotKind.BRAIN,
    name="Built-in rules engine",
    summary="Deterministic diagnosis. No API key, no cost, no network.",
    notes=(
        "Encodes the causal structure of common Kafka failures and scores 5/5 "
        "on the evaluation suite. It cannot explain a failure mode nobody "
        "anticipated — that is what an LLM adds — but it is free, instant, and "
        "always available as the fallback when a model or its budget is not."
    ),
    zero_config=True,
)

ANTHROPIC_BRAIN = Provider(
    id="anthropic",
    slot=SlotKind.BRAIN,
    name="Claude",
    summary="LLM diagnosis for incidents the rules engine cannot classify.",
    notes=(
        "A cheap model triages every incident; the stronger model is consulted "
        "only when triage flags the case as non-obvious or a past remedy failed. "
        "Spending is capped per incident, per hour and per day, and every cap "
        "falls back to the rules engine rather than failing."
    ),
    recommended=True,
    fields=[
        Field("api_key", "Anthropic API key", type="password", secret=True,
              placeholder="sk-ant-...",
              help="Stored server-side and never returned to this page."),
        Field("model_triage", "Triage model", type="select",
              default="claude-haiku-4-5",
              options=[
                  {"value": "claude-haiku-4-5", "label": "Claude Haiku 4.5 — cheapest"},
                  {"value": "claude-sonnet-5", "label": "Claude Sonnet 5"},
              ],
              help="Runs on every incident, so this is where cost accumulates."),
        Field("model_diagnose", "Deep-analysis model", type="select",
              default="claude-opus-5",
              options=[
                  {"value": "claude-opus-5", "label": "Claude Opus 5 — most capable"},
                  {"value": "claude-sonnet-5", "label": "Claude Sonnet 5 — balanced"},
                  {"value": "claude-haiku-4-5", "label": "Claude Haiku 4.5 — cheapest"},
              ],
              help="Only consulted for incidents triage cannot settle."),
        Field("tokens_per_day", "Daily token ceiling", type="number",
              default=400000, required=False,
              help="Once reached, the agent runs on the rules engine until "
                   "the window rolls over. ~$2.40/day at the default."),
    ],
)

# ── notify: where incident notices go ────────────────────────────────

NO_NOTIFY = Provider(
    id="none", slot=SlotKind.NOTIFY, name="Off",
    summary="Incidents are visible in this dashboard only.", zero_config=True,
)

SLACK_NOTIFY = Provider(
    id="slack", slot=SlotKind.NOTIFY, name="Slack",
    summary="Post incidents and approval requests to a Slack channel.",
    notes="Approval requests include the blast radius and the policy's reasoning.",
    recommended=True,
    fields=[
        Field("webhook_url", "Incoming webhook URL", type="password", secret=True,
              placeholder="https://hooks.slack.com/services/...",
              help="Slack → Apps → Incoming Webhooks."),
        Field("notify_on", "Notify on", type="select", default="approvals",
              options=[
                  {"value": "approvals", "label": "Approval requests only"},
                  {"value": "all", "label": "Every incident"},
                  {"value": "unresolved", "label": "Unresolved incidents only"},
              ]),
    ],
)

WEBHOOK_NOTIFY = Provider(
    id="webhook", slot=SlotKind.NOTIFY, name="Generic webhook",
    summary="POST incident JSON to any endpoint.",
    fields=[
        Field("url", "Endpoint URL", placeholder="https://example.com/hooks/guardian"),
        Field("auth_header", "Authorization header", type="password",
              required=False, secret=True, placeholder="Bearer ..."),
        Field("notify_on", "Notify on", type="select", default="all",
              options=[
                  {"value": "all", "label": "Every incident"},
                  {"value": "approvals", "label": "Approval requests only"},
                  {"value": "unresolved", "label": "Unresolved incidents only"},
              ]),
    ],
)

# ── monitor: extra signals ───────────────────────────────────────────

NO_MONITOR = Provider(
    id="none", slot=SlotKind.MONITOR, name="Off",
    summary="Only Kafka and service telemetry are used.", zero_config=True,
)

PROMETHEUS_MONITOR = Provider(
    id="prometheus", slot=SlotKind.MONITOR, name="Prometheus",
    summary="Pull additional metrics from a Prometheus server.",
    notes="Queried alongside Kafka telemetry and fed to the same detectors.",
    fields=[
        Field("base_url", "Prometheus URL", placeholder="http://prometheus:9090"),
        Field("bearer_token", "Bearer token", type="password", required=False,
              secret=True),
        Field("queries", "PromQL queries", type="textarea", required=False,
              placeholder="service_saturation\nupstream_error_ratio",
              help="One query per line. Each becomes a metric the detector watches."),
    ],
)


SLOTS: dict[SlotKind, dict[str, Any]] = {
    SlotKind.SOURCE: {
        "title": "Kafka cluster",
        "description": "Where the agent watches. Start with the demo cluster.",
        "required": True,
        "providers": [DEMO_SOURCE, SELF_HOSTED_SOURCE, CONFLUENT_SOURCE],
        "default": "demo",
    },
    SlotKind.BRAIN: {
        "title": "Decision engine",
        "description": "What diagnoses an incident and decides the remedy.",
        "required": True,
        "providers": [OFFLINE_BRAIN, ANTHROPIC_BRAIN],
        "default": "offline",
    },
    SlotKind.NOTIFY: {
        "title": "Notifications",
        "description": "Where the agent tells people what it did.",
        "required": False,
        "providers": [NO_NOTIFY, SLACK_NOTIFY, WEBHOOK_NOTIFY],
        "default": "none",
    },
    SlotKind.MONITOR: {
        "title": "Extra signals",
        "description": "Optional metrics beyond Kafka's own.",
        "required": False,
        "providers": [NO_MONITOR, PROMETHEUS_MONITOR],
        "default": "none",
    },
}


def provider(slot: SlotKind, provider_id: str) -> Provider | None:
    for p in SLOTS[slot]["providers"]:
        if p.id == provider_id:
            return p
    return None


def mask(value: str) -> str:
    """Render a secret as evidence it exists, without disclosing it."""
    if not value:
        return ""
    tail = value[-4:] if len(value) > 8 else ""
    return f"••••{tail}" if tail else "••••"


def redact(slot: SlotKind, provider_id: str, config: dict[str, Any]) -> dict[str, Any]:
    """Config safe to send to a browser."""
    p = provider(slot, provider_id)
    secrets = p.secret_fields() if p else set()
    return {
        k: (mask(str(v)) if k in secrets and v else v)
        for k, v in config.items()
    }


def catalogue() -> list[dict[str, Any]]:
    return [
        {
            "slot": slot.value,
            "title": meta["title"],
            "description": meta["description"],
            "required": meta["required"],
            "default": meta["default"],
            "providers": [p.to_json() for p in meta["providers"]],
        }
        for slot, meta in SLOTS.items()
    ]
