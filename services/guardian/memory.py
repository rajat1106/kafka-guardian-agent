"""Incident memory.

The agent's recall of what happened before and what fixed it. Two uses:

* **Before diagnosis** — similar past incidents are injected into the
  prompt, so the agent benefits from history instead of re-deriving the
  same conclusion every time. This also *saves tokens*: a matched incident
  usually lets the cheap triage model settle the case alone.
* **After resolution** — the outcome is written back, including whether the
  action actually worked, so a remedy that failed last time is visible next
  time.

Matching is structural (service + signature of which metrics fired) rather
than semantic. At this scale that beats embeddings: it is exact, free, and
needs no vector store.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
import structlog

from guardian_platform.contracts import Incident, Outcome

log = structlog.get_logger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
    incident_id   TEXT PRIMARY KEY,
    opened_at     TIMESTAMPTZ NOT NULL,
    closed_at     TIMESTAMPTZ,
    service       TEXT NOT NULL,
    signature     TEXT NOT NULL,
    state         TEXT NOT NULL,
    severity      TEXT NOT NULL,
    scenario      TEXT,
    root_cause    TEXT,
    confidence    REAL,
    actions_taken JSONB DEFAULT '[]'::jsonb,
    resolved      BOOLEAN DEFAULT FALSE,
    mttr_seconds  REAL,
    tokens_used   INTEGER DEFAULT 0,
    cost_usd      REAL DEFAULT 0,
    human_approved BOOLEAN DEFAULT FALSE,
    payload       JSONB
);
CREATE INDEX IF NOT EXISTS idx_incidents_signature ON incidents (signature);
CREATE INDEX IF NOT EXISTS idx_incidents_service   ON incidents (service);

-- Durable step journal. The agent writes each completed step here, so a
-- crash mid-incident resumes from the last durable step instead of
-- re-executing actions that already ran. This is the lightweight stand-in
-- for a workflow engine; see docs/ARCHITECTURE.md for the Temporal swap.
CREATE TABLE IF NOT EXISTS incident_steps (
    id          BIGSERIAL PRIMARY KEY,
    incident_id TEXT NOT NULL,
    step        TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload     JSONB,
    UNIQUE (incident_id, step)
);
CREATE INDEX IF NOT EXISTS idx_steps_incident ON incident_steps (incident_id);
"""


def signature(incident: Incident) -> str:
    """A stable fingerprint of which signals fired.

    Two incidents match when the same service shows the same set of
    misbehaving metrics — that is what makes them "the same kind of thing"
    even when the numbers differ.
    """
    metrics = sorted({a.metric for a in incident.anomalies})
    return f"{incident.service}:{'+'.join(metrics)}"


class IncidentMemory:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self, retries: int = 30) -> None:
        import asyncio

        for attempt in range(retries):
            try:
                self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=5)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == retries - 1:
                    raise
                log.info("waiting_for_postgres", attempt=attempt + 1, error=str(exc))
                await asyncio.sleep(2)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA)
        log.info("incident_memory_ready")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    # ── recall ───────────────────────────────────────────────────────
    async def similar(self, incident: Incident, limit: int = 3) -> list[dict[str, Any]]:
        """Past incidents with the same signature, most recent first."""
        if not self._pool:
            return []
        sig = signature(incident)
        rows = await self._pool.fetch(
            """
            SELECT incident_id, root_cause, actions_taken, resolved,
                   mttr_seconds, opened_at, confidence
            FROM incidents
            WHERE signature = $1 AND incident_id <> $2 AND root_cause IS NOT NULL
            ORDER BY opened_at DESC
            LIMIT $3
            """,
            sig, incident.incident_id, limit,
        )
        return [
            {
                "incident_id": r["incident_id"],
                "root_cause": r["root_cause"],
                "actions_taken": json.loads(r["actions_taken"]) if r["actions_taken"] else [],
                "resolved": r["resolved"],
                "mttr_seconds": round(r["mttr_seconds"] or 0, 1),
                "confidence": round(r["confidence"] or 0, 2),
            }
            for r in rows
        ]

    # ── record ───────────────────────────────────────────────────────
    async def open_incident(self, incident: Incident) -> None:
        if not self._pool:
            return
        await self._pool.execute(
            """
            INSERT INTO incidents (incident_id, opened_at, service, signature,
                                   state, severity, scenario, payload)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (incident_id) DO NOTHING
            """,
            incident.incident_id, incident.opened_at, incident.service,
            signature(incident), incident.state.value, incident.severity.value,
            incident.scenario, json.dumps(incident.model_dump(mode="json")),
        )

    async def close_incident(self, incident: Incident, outcome: Outcome) -> None:
        if not self._pool:
            return
        await self._pool.execute(
            """
            UPDATE incidents
               SET closed_at = now(), state = $2, root_cause = $3, confidence = $4,
                   actions_taken = $5, resolved = $6, mttr_seconds = $7,
                   tokens_used = $8, cost_usd = $9, human_approved = $10,
                   payload = $11
             WHERE incident_id = $1
            """,
            incident.incident_id, incident.state.value, outcome.root_cause,
            incident.diagnosis.confidence if incident.diagnosis else 0.0,
            json.dumps(outcome.actions_taken), outcome.resolved,
            outcome.mttr_seconds, outcome.tokens_used, outcome.cost_usd,
            outcome.human_approved,
            json.dumps(incident.model_dump(mode="json")),
        )

    # ── durable journal ──────────────────────────────────────────────
    async def record_step(self, incident_id: str, step: str, payload: dict | None = None) -> None:
        if not self._pool:
            return
        await self._pool.execute(
            """
            INSERT INTO incident_steps (incident_id, step, payload)
            VALUES ($1,$2,$3)
            ON CONFLICT (incident_id, step) DO NOTHING
            """,
            incident_id, step, json.dumps(payload or {}),
        )

    async def completed_steps(self, incident_id: str) -> set[str]:
        """Steps already durably completed — the crash-resume read path."""
        if not self._pool:
            return set()
        rows = await self._pool.fetch(
            "SELECT step FROM incident_steps WHERE incident_id = $1", incident_id
        )
        return {r["step"] for r in rows}

    async def unfinished_incidents(self) -> list[str]:
        """Incidents that were open when the agent last stopped."""
        if not self._pool:
            return []
        rows = await self._pool.fetch(
            "SELECT incident_id FROM incidents WHERE closed_at IS NULL "
            "ORDER BY opened_at DESC LIMIT 20"
        )
        return [r["incident_id"] for r in rows]

    # ── reporting ────────────────────────────────────────────────────
    async def stats(self) -> dict[str, Any]:
        if not self._pool:
            return {}
        row = await self._pool.fetchrow(
            """
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE resolved) AS resolved,
                   COUNT(*) FILTER (WHERE human_approved) AS approved,
                   AVG(mttr_seconds) FILTER (WHERE resolved) AS avg_mttr,
                   COALESCE(SUM(cost_usd), 0) AS total_cost,
                   COALESCE(SUM(tokens_used), 0) AS total_tokens
            FROM incidents
            """
        )
        return {
            "incidents_total": row["total"],
            "incidents_resolved": row["resolved"],
            "human_approvals": row["approved"],
            "avg_mttr_seconds": round(row["avg_mttr"] or 0, 1),
            "total_cost_usd": round(float(row["total_cost"] or 0), 4),
            "total_tokens": int(row["total_tokens"] or 0),
        }
