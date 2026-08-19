"""Chaos engine — injects the scenarios that give the agent work to do."""

from __future__ import annotations

import asyncio
import os
import random
import sys

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException

sys.path.insert(0, "/app/platform")

from guardian_platform.config import app_settings  # noqa: E402
from guardian_platform.obs import configure_logging  # noqa: E402

from scenarios import ALL_SCENARIOS, BY_KEY  # noqa: E402

log = configure_logging("chaos")
FLEET_URL = os.getenv("FLEET_URL", "http://fleet:8081")

app = FastAPI(title="Guardian Chaos Engine", version="2.0.0")
_state = {"enabled": app_settings().chaos_enabled, "last": None, "count": 0}


async def _inject(key: str) -> dict:
    scenario = BY_KEY.get(key)
    if scenario is None:
        raise HTTPException(404, f"unknown scenario {key!r}; have {sorted(BY_KEY)}")
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{FLEET_URL}/services/{scenario.service}/fault", json=scenario.fault
        )
        resp.raise_for_status()
    _state["last"] = key
    _state["count"] += 1
    log.info("chaos_injected", scenario=key, service=scenario.service,
             title=scenario.title)
    return {"scenario": key, "service": scenario.service, "title": scenario.title}


@app.get("/health")
async def health() -> dict:
    return {"ok": True, **_state}


@app.get("/scenarios")
async def scenarios() -> dict:
    return {
        "scenarios": [
            {"key": s.key, "title": s.title, "service": s.service,
             "description": s.description, "ground_truth": s.ground_truth,
             "acceptable_actions": s.acceptable_actions}
            for s in ALL_SCENARIOS
        ]
    }


@app.post("/inject/{key}")
async def inject(key: str) -> dict:
    return await _inject(key)


@app.post("/toggle")
async def toggle() -> dict:
    _state["enabled"] = not _state["enabled"]
    return {"enabled": _state["enabled"]}


async def _auto_loop() -> None:
    """Periodically inject a random scenario so the demo is never idle."""
    interval = app_settings().chaos_interval_seconds
    # Let the fleet establish a baseline before breaking anything, otherwise
    # the detector has nothing to compare against and flags the cold start.
    await asyncio.sleep(max(interval, 45))
    while True:
        if _state["enabled"]:
            try:
                await _inject(random.choice(ALL_SCENARIOS).key)
            except Exception as exc:  # noqa: BLE001
                log.warning("auto_inject_failed", error=str(exc))
        await asyncio.sleep(interval)


@app.on_event("startup")
async def startup() -> None:
    log.info("chaos_starting", fleet=FLEET_URL,
             interval=app_settings().chaos_interval_seconds,
             enabled=_state["enabled"])
    app.state.task = asyncio.create_task(_auto_loop())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082, log_config=None)
