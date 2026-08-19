"""Connection tests and discovery.

Every test performs a real operation against the real thing — a Kafka
metadata request, an actual Claude call, an actual webhook POST. A test that
only validates the shape of a form field tells the user nothing they did not
already know, and lets a broken integration reach production looking green.

Each test returns a small dict that the UI renders verbatim, including the
concrete failure text when it fails. Kafka authentication failures in
particular are unhelpful unless you see the underlying error.
"""

from __future__ import annotations

import asyncio
import os
import ssl
import time
from typing import Any

import httpx
import structlog

from guardian_platform.plugins import SlotKind

log = structlog.get_logger(__name__)

DEMO_BOOTSTRAP = os.getenv("DEMO_BOOTSTRAP", "kafka:9092")
FLEET_URL = os.getenv("FLEET_URL", "http://fleet:8081").rstrip("/")
CHAOS_URL = os.getenv("CHAOS_URL", "http://chaos:8082").rstrip("/")


# ── Kafka ────────────────────────────────────────────────────────────

def _kafka_kwargs(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    if provider_id == "demo":
        return {"bootstrap_servers": DEMO_BOOTSTRAP, "security_protocol": "PLAINTEXT"}

    if provider_id == "confluent":
        protocol, mechanism = "SASL_SSL", "PLAIN"
    else:
        protocol = cfg.get("security_protocol") or "PLAINTEXT"
        mechanism = cfg.get("sasl_mechanism") or "PLAIN"

    kwargs: dict[str, Any] = {
        "bootstrap_servers": cfg.get("bootstrap_servers", ""),
        "security_protocol": protocol,
        "request_timeout_ms": 20_000,
    }
    if "SASL" in protocol.upper():
        kwargs["sasl_mechanism"] = mechanism
        kwargs["sasl_plain_username"] = cfg.get("sasl_username", "")
        kwargs["sasl_plain_password"] = cfg.get("sasl_password", "")
    if "SSL" in protocol.upper():
        kwargs["ssl_context"] = ssl.create_default_context()
    return kwargs


async def test_kafka(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """Connect, list topics, describe consumer groups."""
    from aiokafka.admin import AIOKafkaAdminClient

    started = time.perf_counter()
    if provider_id != "demo" and not cfg.get("bootstrap_servers"):
        return {"ok": False, "error": "Bootstrap servers are required."}
    if provider_id == "confluent" and not (
        cfg.get("sasl_username") and cfg.get("sasl_password")
    ):
        return {"ok": False,
                "error": "Confluent Cloud needs a cluster API key and secret."}

    admin = AIOKafkaAdminClient(client_id="kga-conn-test",
                                **_kafka_kwargs(provider_id, cfg))
    try:
        await asyncio.wait_for(admin.start(), timeout=25)
        topics = sorted(await admin.list_topics())
        groups = await admin.list_consumer_groups()
        elapsed = int((time.perf_counter() - started) * 1000)
        return {
            "ok": True,
            "latency_ms": elapsed,
            "topics": len(topics),
            "consumer_groups": len(groups),
            "summary": (f"Connected in {elapsed} ms — {len(topics)} topics, "
                        f"{len(groups)} consumer groups."),
        }
    except asyncio.TimeoutError:
        return {"ok": False,
                "error": "Timed out after 25s. Check the bootstrap address is "
                         "reachable from this host and any firewall allows it."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        try:
            await admin.close()
        except Exception:  # noqa: BLE001
            pass


async def discover_kafka(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """Fetch the cluster's topics, partitions and consumer groups."""
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(client_id="kga-discover",
                                **_kafka_kwargs(provider_id, cfg))
    try:
        await asyncio.wait_for(admin.start(), timeout=25)
        names = sorted(await admin.list_topics())
        described = await admin.describe_topics(names) if names else []
        prefix = cfg.get("topic_prefix", "")

        topics = []
        for t in described:
            parts = t.get("partitions", [])
            name = t["topic"]
            topics.append({
                "name": name,
                "partitions": len(parts),
                "replication_factor": len(parts[0]["replicas"]) if parts else 0,
                "internal": name.startswith("__"),
                # Distinguishes the agent's own plumbing from the user's data.
                "owned_by_guardian": name.startswith(f"{prefix}guardian.")
                                     or name.startswith(f"{prefix}telemetry."),
            })

        groups = [
            {"group_id": g[0], "protocol_type": g[1]}
            for g in await admin.list_consumer_groups()
        ]
        return {
            "ok": True,
            "topics": topics,
            "consumer_groups": groups,
            "total_partitions": sum(t["partitions"] for t in topics),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    finally:
        try:
            await admin.close()
        except Exception:  # noqa: BLE001
            pass


# ── brain ────────────────────────────────────────────────────────────

async def test_brain(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """Make a real, deliberately tiny model call."""
    if provider_id == "offline":
        return {"ok": True, "summary": "Rules engine is always available.",
                "cost_usd": 0.0}

    key = (cfg.get("api_key") or "").strip()
    if not key:
        return {"ok": False, "error": "An API key is required."}

    import anthropic

    model = cfg.get("model_triage") or "claude-haiku-4-5"
    started = time.perf_counter()
    client = anthropic.AsyncAnthropic(api_key=key, timeout=30.0, max_retries=1)
    try:
        # Smallest call that still proves the key, the model name and the
        # network path all work.
        resp = await client.messages.create(
            model=model,
            max_tokens=16,
            messages=[{"role": "user",
                       "content": "Reply with the single word: ready"}],
        )
        elapsed = int((time.perf_counter() - started) * 1000)
        text = "".join(b.text for b in resp.content if b.type == "text").strip()
        usage = resp.usage
        from budget import cost_usd  # local import: keeps this module importable alone

        spend = cost_usd(model, usage.input_tokens, usage.output_tokens)
        return {
            "ok": True,
            "latency_ms": elapsed,
            "model": model,
            "reply": text[:40],
            "tokens": usage.input_tokens + usage.output_tokens,
            "cost_usd": round(spend, 6),
            "summary": (f"{model} responded in {elapsed} ms "
                        f"({usage.input_tokens + usage.output_tokens} tokens, "
                        f"${spend:.6f})."),
        }
    except anthropic.AuthenticationError:
        return {"ok": False,
                "error": "The API key was rejected. Check for a copied space "
                         "or a key from a different organisation."}
    except anthropic.NotFoundError:
        return {"ok": False,
                "error": f"Model {model!r} is not available to this key."}
    except anthropic.RateLimitError:
        return {"ok": False,
                "error": "Rate limited. The key works, but the account has no "
                         "capacity right now — try again shortly."}
    except anthropic.APIStatusError as exc:
        return {"ok": False, "error": f"API error {exc.status_code}: {exc.message}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ── notify ───────────────────────────────────────────────────────────

async def test_notify(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    """Send a real message, so the user sees it arrive."""
    if provider_id == "none":
        return {"ok": True, "summary": "Notifications are off."}

    text = ("Kafka Guardian connected. This is a test message — "
            "incident notices will arrive here.")
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            if provider_id == "slack":
                url = cfg.get("webhook_url", "")
                if not url:
                    return {"ok": False, "error": "A webhook URL is required."}
                r = await client.post(url, json={"text": f":shield: {text}"})
            else:
                url = cfg.get("url", "")
                if not url:
                    return {"ok": False, "error": "An endpoint URL is required."}
                headers = {}
                if cfg.get("auth_header"):
                    headers["Authorization"] = cfg["auth_header"]
                r = await client.post(
                    url, headers=headers,
                    json={"event": "connection_test", "source": "kafka-guardian",
                          "message": text},
                )
        if r.status_code >= 400:
            return {"ok": False,
                    "error": f"Endpoint returned {r.status_code}: {r.text[:180]}"}
        return {"ok": True, "status_code": r.status_code,
                "summary": f"Test message delivered ({r.status_code}). Check the channel."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ── monitor ──────────────────────────────────────────────────────────

async def test_monitor(provider_id: str, cfg: dict[str, Any]) -> dict[str, Any]:
    if provider_id == "none":
        return {"ok": True, "summary": "No extra signals configured."}
    base = (cfg.get("base_url") or "").rstrip("/")
    if not base:
        return {"ok": False, "error": "A Prometheus URL is required."}
    headers = {}
    if cfg.get("bearer_token"):
        headers["Authorization"] = f"Bearer {cfg['bearer_token']}"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(f"{base}/api/v1/query",
                                 params={"query": "up"}, headers=headers)
        if r.status_code >= 400:
            return {"ok": False,
                    "error": f"Prometheus returned {r.status_code}: {r.text[:180]}"}
        payload = r.json()
        series = len(payload.get("data", {}).get("result", []))
        return {"ok": True, "series": series,
                "summary": f"Reachable — {series} series match `up`."}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


TESTERS = {
    SlotKind.SOURCE: test_kafka,
    SlotKind.BRAIN: test_brain,
    SlotKind.NOTIFY: test_notify,
    SlotKind.MONITOR: test_monitor,
}
