"""Tamper-evident audit log.

Every consequential event — an approval, a configuration change, an executed
action, a rollback — is appended here as a hash-chained record:

    hash(n) = sha256( hash(n-1) || canonical_json(record(n)) )

Altering or deleting any record breaks every hash after it, so tampering is
detectable even by someone with write access to the table. That is the
difference between a log and an audit log.

Two properties this deliberately does *not* claim:

* It is not tamper-**proof**. Someone with database write access can rewrite
  the whole chain from the point of edit onward. Making that infeasible needs
  the head hash anchored somewhere they do not control — periodically
  published to an external store, signed with a KMS key, or written to WORM
  storage. `head()` exists to be anchored; anchoring is deployment-specific.
* It is not a substitute for the Kafka topics. Those remain the operational
  event stream, with short retention. This is the durable record, kept for as
  long as compliance requires.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import asyncpg
import structlog

log = structlog.get_logger(__name__)

GENESIS = "0" * 64

SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_log (
    seq         BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    event       TEXT NOT NULL,
    actor       TEXT NOT NULL,
    actor_name  TEXT,
    actor_issuer TEXT,
    subject     TEXT,
    detail      JSONB NOT NULL DEFAULT '{}'::jsonb,
    prev_hash   TEXT NOT NULL,
    hash        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_ts      ON audit_log (ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event   ON audit_log (event);
CREATE INDEX IF NOT EXISTS idx_audit_subject ON audit_log (subject);

-- Appends only. An UPDATE or DELETE against the audit log is either a bug or
-- an attack; refusing both at the database level means application code
-- cannot get it wrong.
CREATE OR REPLACE FUNCTION audit_log_immutable() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'audit_log is append-only (attempted %)', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_log_no_update ON audit_log;
CREATE TRIGGER audit_log_no_update BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION audit_log_immutable();
"""


class AuditEvent(str, Enum):
    LOGIN = "auth.login"
    LOGIN_FAILED = "auth.login_failed"
    APPROVAL_GRANTED = "approval.granted"
    APPROVAL_DENIED = "approval.denied"
    APPROVAL_FORBIDDEN = "approval.forbidden"
    ACTION_EXECUTED = "action.executed"
    ACTION_BLOCKED = "action.blocked"
    ACTION_ROLLED_BACK = "action.rolled_back"
    PLUGIN_CONFIGURED = "plugin.configured"
    PLUGIN_TESTED = "plugin.tested"
    CHAOS_INJECTED = "chaos.injected"
    AUTONOMY_CHANGED = "autonomy.changed"


def _canonical(payload: dict[str, Any]) -> str:
    """Stable serialisation — key order and separators must never vary."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def compute_hash(prev_hash: str, record: dict[str, Any]) -> str:
    return hashlib.sha256(
        (prev_hash + _canonical(record)).encode()
    ).hexdigest()


class AuditLog:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self, retries: int = 30) -> None:
        import asyncio

        for attempt in range(retries):
            try:
                self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=4)
                break
            except Exception:  # noqa: BLE001
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA)
        log.info("audit_log_ready")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def record(
        self,
        event: AuditEvent,
        actor: str,
        detail: dict[str, Any] | None = None,
        subject: str | None = None,
        actor_name: str | None = None,
        actor_issuer: str | None = None,
    ) -> dict[str, Any]:
        """Append one record and return it, including its hash."""
        if not self._pool:
            return {}
        ts = datetime.now(timezone.utc)
        payload = {
            "event": event.value, "actor": actor, "actor_name": actor_name,
            "actor_issuer": actor_issuer, "subject": subject,
            "detail": detail or {}, "ts": ts.isoformat(),
        }
        # Serialised so two concurrent appends cannot both read the same head
        # and fork the chain.
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("LOCK TABLE audit_log IN EXCLUSIVE MODE")
                prev = await conn.fetchval(
                    "SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1"
                ) or GENESIS
                digest = compute_hash(prev, payload)
                seq = await conn.fetchval(
                    """
                    INSERT INTO audit_log
                        (ts, event, actor, actor_name, actor_issuer, subject,
                         detail, prev_hash, hash)
                    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9)
                    RETURNING seq
                    """,
                    ts, event.value, actor, actor_name, actor_issuer, subject,
                    json.dumps(detail or {}), prev, digest,
                )
        return {"seq": seq, "hash": digest, "prev_hash": prev, **payload}

    async def verify(self, limit: int | None = None) -> dict[str, Any]:
        """Walk the chain and report the first break, if any."""
        if not self._pool:
            return {"ok": False, "error": "audit log unavailable"}
        rows = await self._pool.fetch(
            "SELECT seq, ts, event, actor, actor_name, actor_issuer, subject, "
            "detail, prev_hash, hash FROM audit_log ORDER BY seq ASC"
            + (f" LIMIT {int(limit)}" if limit else "")
        )
        expected_prev = GENESIS
        for r in rows:
            detail = r["detail"]
            payload = {
                "event": r["event"], "actor": r["actor"],
                "actor_name": r["actor_name"], "actor_issuer": r["actor_issuer"],
                "subject": r["subject"],
                "detail": json.loads(detail) if isinstance(detail, str) else detail,
                "ts": r["ts"].isoformat(),
            }
            if r["prev_hash"] != expected_prev:
                return {"ok": False, "records": len(rows), "broken_at": r["seq"],
                        "reason": "prev_hash does not match the preceding record"}
            if compute_hash(expected_prev, payload) != r["hash"]:
                return {"ok": False, "records": len(rows), "broken_at": r["seq"],
                        "reason": "record content does not match its hash"}
            expected_prev = r["hash"]
        return {"ok": True, "records": len(rows), "head": expected_prev}

    async def head(self) -> str:
        """Current chain head — the value to anchor externally."""
        if not self._pool:
            return GENESIS
        return await self._pool.fetchval(
            "SELECT hash FROM audit_log ORDER BY seq DESC LIMIT 1"
        ) or GENESIS

    async def recent(
        self, limit: int = 100, event: str | None = None,
        subject: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._pool:
            return []
        clauses, args = [], []
        if event:
            args.append(event)
            clauses.append(f"event = ${len(args)}")
        if subject:
            args.append(subject)
            clauses.append(f"subject = ${len(args)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        args.append(limit)
        rows = await self._pool.fetch(
            f"SELECT seq, ts, event, actor, actor_name, actor_issuer, subject, "
            f"detail, hash FROM audit_log {where} ORDER BY seq DESC LIMIT ${len(args)}",
            *args,
        )
        return [
            {
                "seq": r["seq"], "ts": r["ts"].isoformat(), "event": r["event"],
                "actor": r["actor"], "actor_name": r["actor_name"],
                "actor_issuer": r["actor_issuer"], "subject": r["subject"],
                "detail": (json.loads(r["detail"])
                           if isinstance(r["detail"], str) else r["detail"]),
                "hash": r["hash"],
            }
            for r in rows
        ]
