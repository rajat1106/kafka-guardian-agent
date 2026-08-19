"""Demo fleet: runs the simulated services and publishes their metrics.

Also exposes the control API that the SimulatedActuator drives. In the
real world these operations would be kubectl calls; here they mutate
simulation state. The interface is identical either way, which is the
whole point of the actuator abstraction.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sys
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

sys.path.insert(0, "/app/platform")

from guardian_platform.config import kafka_settings  # noqa: E402
from guardian_platform.contracts import ServiceMetrics  # noqa: E402
from guardian_platform.kafka import EventProducer, ensure_topics  # noqa: E402
from guardian_platform.obs import configure_logging  # noqa: E402
from guardian_platform.topics import TELEMETRY_METRICS  # noqa: E402

from simulation import build_fleet  # noqa: E402

log = configure_logging("fleet")
FLEET = build_fleet()
TICK_SECONDS = float(os.getenv("FLEET_TICK_SECONDS", "2"))

app = FastAPI(title="Guardian Demo Fleet", version="2.0.0")
_producer: EventProducer | None = None
_latest: dict[str, ServiceMetrics] = {}


class ScaleRequest(BaseModel):
    replicas: int


class PoolRequest(BaseModel):
    size: int


class PartitionRequest(BaseModel):
    partitions: int


class ThrottleRequest(BaseModel):
    factor: float


class FaultRequest(BaseModel):
    label: str
    ttl_ticks: int = 30
    memory_leak_mb_per_tick: float = 0.0
    pool_leak_per_tick: float = 0.0
    consumer_slowdown: float = 1.0
    upstream_latency_ms: float = 0.0
    partition_offline: bool = False
    region_down: bool = False


def _svc(name: str):
    if name not in FLEET:
        raise HTTPException(404, f"unknown service {name!r}; have {sorted(FLEET)}")
    return FLEET[name]


@app.get("/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "services": sorted(FLEET), "kafka": kafka_settings().describe()}


@app.get("/services")
async def services() -> dict[str, Any]:
    return {
        name: {
            "replicas": s.replicas,
            "partition_count": s.partition_count,
            "db_pool_size": s.db_pool_size,
            "consumer_group": s.consumer_group,
            "healthy": s.healthy,
            "region": s.region,
            "active_fault": s.faults.label,
            "latest": _latest.get(name).model_dump(mode="json") if name in _latest else None,
        }
        for name, s in FLEET.items()
    }


@app.post("/services/{name}/scale")
async def scale(name: str, req: ScaleRequest) -> dict[str, Any]:
    s = _svc(name)
    before = s.replicas
    s.scale(req.replicas)
    log.info("scaled", service=name, before=before, after=s.replicas)
    return {"service": name, "replicas_before": before, "replicas_after": s.replicas}


@app.post("/services/{name}/pool")
async def pool(name: str, req: PoolRequest) -> dict[str, Any]:
    s = _svc(name)
    before = s.db_pool_size
    s.resize_pool(req.size)
    log.info("pool_resized", service=name, before=before, after=s.db_pool_size)
    return {"service": name, "pool_before": before, "pool_after": s.db_pool_size}


@app.post("/services/{name}/partitions")
async def partitions(name: str, req: PartitionRequest) -> dict[str, Any]:
    s = _svc(name)
    before = s.partition_count
    s.add_partitions(req.partitions)
    return {"service": name, "partitions_before": before, "partitions_after": s.partition_count}


@app.post("/services/{name}/restart")
async def restart(name: str) -> dict[str, Any]:
    s = _svc(name)
    s.restart()
    log.info("restarted", service=name)
    return {"service": name, "restarted": True}


@app.post("/services/{name}/clear-backlog")
async def clear_backlog(name: str) -> dict[str, Any]:
    s = _svc(name)
    before = s.consumer_lag
    s.clear_backlog()
    return {"service": name, "lag_before": before, "lag_after": s.consumer_lag}


@app.post("/services/{name}/throttle")
async def throttle(name: str, req: ThrottleRequest) -> dict[str, Any]:
    s = _svc(name)
    s.throttle(req.factor)
    return {"service": name, "throttle_factor": s.throttle_factor}


@app.post("/services/{name}/fault")
async def inject_fault(name: str, req: FaultRequest) -> dict[str, Any]:
    """Used by the chaos engine. Not something the agent can call."""
    s = _svc(name)
    s.faults.label = req.label
    s.faults.ttl_ticks = req.ttl_ticks
    s.faults.memory_leak_mb_per_tick = req.memory_leak_mb_per_tick
    s.faults.pool_leak_per_tick = req.pool_leak_per_tick
    s.faults.consumer_slowdown = req.consumer_slowdown
    s.faults.upstream_latency_ms = req.upstream_latency_ms
    s.faults.partition_offline = req.partition_offline
    s.faults.region_down = req.region_down
    log.info("fault_injected", service=name, label=req.label, ttl=req.ttl_ticks)
    return {"service": name, "fault": req.label}


@app.post("/services/{name}/heal")
async def heal(name: str) -> dict[str, Any]:
    s = _svc(name)
    s.faults.clear()
    return {"service": name, "cleared": True}


@app.post("/reset")
async def reset() -> dict[str, Any]:
    global FLEET
    FLEET = build_fleet()
    _latest.clear()
    return {"reset": True}


async def _tick_loop() -> None:
    """Advance the simulation and publish metrics forever."""
    assert _producer is not None
    while True:
        try:
            for name, svc in FLEET.items():
                metrics = svc.step()
                _latest[name] = metrics
                await _producer.send(TELEMETRY_METRICS, metrics, key=name)
        except Exception as exc:  # noqa: BLE001 — the tick loop must survive broker blips
            log.warning("tick_failed", error=str(exc))
        await asyncio.sleep(TICK_SECONDS)


@app.on_event("startup")
async def startup() -> None:
    global _producer
    settings = kafka_settings()
    log.info("fleet_starting", kafka=settings.describe(), tick_seconds=TICK_SECONDS)
    await ensure_topics(settings)
    _producer = await EventProducer("fleet").start()
    app.state.tick_task = asyncio.create_task(_tick_loop())


@app.on_event("shutdown")
async def shutdown() -> None:
    task = getattr(app.state, "tick_task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if _producer:
        await _producer.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081, log_config=None)
