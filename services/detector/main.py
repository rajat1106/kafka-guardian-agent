"""Detector service: telemetry in, anomalies out. Never calls an LLM."""

from __future__ import annotations

import asyncio
import contextlib
import sys

import uvicorn
from fastapi import FastAPI

sys.path.insert(0, "/app/platform")

from guardian_platform.config import kafka_settings  # noqa: E402
from guardian_platform.contracts import ServiceMetrics  # noqa: E402
from guardian_platform.kafka import EventConsumer, EventProducer, ensure_topics  # noqa: E402
from guardian_platform.obs import configure_logging  # noqa: E402
from guardian_platform.topics import GUARDIAN_ANOMALIES, TELEMETRY_METRICS  # noqa: E402

from detection import Detector  # noqa: E402

log = configure_logging("detector")
app = FastAPI(title="Guardian Detector", version="2.0.0")

_detector = Detector()
_producer: EventProducer | None = None
_stats = {"observed": 0, "anomalies": 0}


@app.get("/health")
async def health() -> dict:
    return {"ok": True, "forest_backend": _detector.forest_backend, **_stats}


async def _run() -> None:
    global _producer
    consumer = EventConsumer([TELEMETRY_METRICS], "guardian-detector", ServiceMetrics)
    await consumer.start()
    log.info("detector_running", forest_backend=_detector.forest_backend)
    try:
        async for metrics in consumer:
            _stats["observed"] += 1
            for anomaly in _detector.observe(metrics):
                _stats["anomalies"] += 1
                assert _producer is not None
                await _producer.send(GUARDIAN_ANOMALIES, anomaly, key=anomaly.service)
                log.info("anomaly", service=anomaly.service, metric=anomaly.metric,
                         detector=anomaly.detector, score=anomaly.score,
                         severity=anomaly.severity.value)
    finally:
        await consumer.stop()


@app.on_event("startup")
async def startup() -> None:
    global _producer
    ks = kafka_settings()
    log.info("detector_starting", kafka=ks.describe())
    await ensure_topics(ks)
    _producer = await EventProducer("detector").start()
    app.state.task = asyncio.create_task(_run())


@app.on_event("shutdown")
async def shutdown() -> None:
    task = getattr(app.state, "task", None)
    if task:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    if _producer:
        await _producer.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8084, log_config=None)
