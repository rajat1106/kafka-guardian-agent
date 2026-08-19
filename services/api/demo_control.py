"""Lifecycle control for the bundled demo cluster.

Talks to the Docker daemon through the mounted socket, so "connect the demo
cluster" starts a real Apache Kafka broker rather than flipping a flag. The
same access lets chaos reach the container itself: pausing or restarting the
broker produces failures no in-process simulation can — a genuine partition
leader election, real client reconnects, real under-replicated partitions.

The socket mount is the reason this is scoped tightly: only containers whose
names carry the project prefix are visible to any operation here, and the
verbs are limited to start/stop/pause/restart. Anything else on the host is
neither listed nor reachable through this module.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import structlog

log = structlog.get_logger(__name__)

PROJECT_PREFIX = os.getenv("DEMO_CONTAINER_PREFIX", "kga-")
# The containers that constitute the demo cluster, in start order.
DEMO_SERVICES = ["kafka", "fleet", "chaos", "detector"]
BROKER = "kafka"

_ALLOWED_VERBS = {"start", "stop", "restart", "pause", "unpause"}


def _client():
    import docker

    return docker.from_env()


def _name(service: str) -> str:
    return f"{PROJECT_PREFIX}{service}"


def _guard(name: str) -> None:
    """Refuse to touch anything outside this project."""
    if not name.startswith(PROJECT_PREFIX):
        raise PermissionError(
            f"{name!r} is outside the demo project prefix {PROJECT_PREFIX!r}; refusing"
        )


def _sync_status() -> dict[str, Any]:
    try:
        client = _client()
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "error": f"Docker unreachable: {exc}",
                "containers": []}

    out = []
    for service in DEMO_SERVICES:
        name = _name(service)
        try:
            c = client.containers.get(name)
            health = (c.attrs.get("State", {}).get("Health") or {}).get("Status")
            out.append({
                "service": service, "name": name, "status": c.status,
                "health": health, "running": c.status == "running",
                "image": (c.image.tags or ["?"])[0],
            })
        except Exception:  # noqa: BLE001 — container may simply not exist yet
            out.append({"service": service, "name": name, "status": "absent",
                        "health": None, "running": False, "image": None})
    running = sum(1 for c in out if c["running"])
    return {
        "available": True,
        "containers": out,
        "running": running,
        "total": len(DEMO_SERVICES),
        "all_up": running == len(DEMO_SERVICES),
    }


def _sync_act(service: str, verb: str, timeout: int = 30) -> dict[str, Any]:
    if verb not in _ALLOWED_VERBS:
        return {"ok": False, "error": f"verb {verb!r} is not permitted"}
    name = _name(service)
    _guard(name)
    try:
        client = _client()
        c = client.containers.get(name)
        getattr(c, verb)(**({"timeout": timeout} if verb in ("stop", "restart") else {}))
        c.reload()
        return {"ok": True, "service": service, "verb": verb, "status": c.status}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "service": service, "verb": verb,
                "error": f"{type(exc).__name__}: {exc}"}


async def status() -> dict[str, Any]:
    return await asyncio.to_thread(_sync_status)


async def act(service: str, verb: str) -> dict[str, Any]:
    result = await asyncio.to_thread(_sync_act, service, verb)
    log.info("demo_container_action", **result)
    return result


async def start_cluster() -> dict[str, Any]:
    """Bring the demo cluster up, broker first."""
    results = []
    for service in DEMO_SERVICES:
        results.append(await act(service, "start"))
        if service == BROKER:
            # The rest are useless until the broker accepts connections.
            await asyncio.sleep(6)
    return {"ok": all(r["ok"] for r in results), "steps": results,
            "status": await status()}


async def stop_cluster() -> dict[str, Any]:
    results = [await act(s, "stop") for s in reversed(DEMO_SERVICES)]
    return {"ok": all(r["ok"] for r in results), "steps": results,
            "status": await status()}


# ── infrastructure-level chaos ───────────────────────────────────────
# These are qualitatively different from the simulated faults: they break the
# actual broker process, so clients reconnect for real and partitions really
# do lose their leader.

INFRA_SCENARIOS = {
    "broker_pause": {
        "title": "Freeze the Kafka broker",
        "description": ("SIGSTOPs the broker container. Producers and consumers "
                        "stall, the controller loses its heartbeat, and the "
                        "cluster is genuinely unavailable — not a simulation of "
                        "unavailability."),
        "service": BROKER, "verb": "pause", "recovery": "unpause",
        "danger": "high",
    },
    "broker_restart": {
        "title": "Restart the Kafka broker",
        "description": ("Stops and restarts the broker container, forcing a "
                        "real partition leader election and client reconnects."),
        "service": BROKER, "verb": "restart", "recovery": None,
        "danger": "high",
    },
    "consumer_kill": {
        "title": "Kill the consumer fleet",
        "description": ("Stops the demo services outright. Consumer groups go "
                        "empty and lag climbs against a live broker."),
        "service": "fleet", "verb": "stop", "recovery": "start",
        "danger": "medium",
    },
}


async def inject_infra(key: str) -> dict[str, Any]:
    scenario = INFRA_SCENARIOS.get(key)
    if scenario is None:
        return {"ok": False, "error": f"unknown infrastructure scenario {key!r}"}
    result = await act(scenario["service"], scenario["verb"])
    return {**result, "scenario": key, "title": scenario["title"],
            "recovery": scenario["recovery"]}


async def recover_infra(key: str) -> dict[str, Any]:
    scenario = INFRA_SCENARIOS.get(key)
    if scenario is None or not scenario.get("recovery"):
        return {"ok": False, "error": f"{key!r} has no recovery verb"}
    return await act(scenario["service"], scenario["recovery"])
