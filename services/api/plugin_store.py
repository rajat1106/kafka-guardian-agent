"""Persistence for plugin configuration.

Secrets live here and nowhere else that a browser can reach. `public_view()`
is the only shape ever serialised to the frontend, and it masks every field
the provider marked secret.

Storage note: values are held as JSON in Postgres. For anything beyond a local
demo the database should be encrypted at rest and the API should sit behind
authentication — a plugin store is, by construction, a credential store.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from guardian_platform.plugins import SLOTS, SlotKind, Status, provider, redact

log = structlog.get_logger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS plugin_config (
    slot        TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}'::jsonb,
    status      TEXT NOT NULL DEFAULT 'unconfigured',
    last_test   JSONB,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


class PluginStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self, retries: int = 30) -> None:
        import asyncio

        for attempt in range(retries):
            try:
                self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=4)
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == retries - 1:
                    raise
                log.info("plugin_store_waiting", attempt=attempt + 1, error=str(exc))
                await asyncio.sleep(2)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA)
        await self._seed_defaults()
        log.info("plugin_store_ready")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def _seed_defaults(self) -> None:
        """Every slot always has a row, so the UI never renders an empty state."""
        assert self._pool is not None
        for slot, meta in SLOTS.items():
            default_id = meta["default"]
            p = provider(slot, default_id)
            # A zero-config default is connected the moment it exists; there is
            # nothing to test and nothing for the user to supply.
            status = Status.CONNECTED.value if (p and p.zero_config) else Status.UNCONFIGURED.value
            await self._pool.execute(
                """
                INSERT INTO plugin_config (slot, provider_id, config, status)
                VALUES ($1, $2, '{}'::jsonb, $3)
                ON CONFLICT (slot) DO NOTHING
                """,
                slot.value, default_id, status,
            )

    # ── reads ────────────────────────────────────────────────────────
    async def raw(self, slot: SlotKind) -> dict[str, Any] | None:
        """Full config including secrets. Server-side callers only."""
        if not self._pool:
            return None
        row = await self._pool.fetchrow(
            "SELECT slot, provider_id, config, status, last_test, updated_at "
            "FROM plugin_config WHERE slot = $1", slot.value
        )
        if row is None:
            return None
        return {
            "slot": row["slot"],
            "provider_id": row["provider_id"],
            "config": json.loads(row["config"]) if isinstance(row["config"], str) else row["config"],
            "status": row["status"],
            "last_test": (json.loads(row["last_test"])
                          if isinstance(row["last_test"], str) else row["last_test"]),
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }

    async def public_view(self) -> list[dict[str, Any]]:
        """The only shape the browser ever sees."""
        out: list[dict[str, Any]] = []
        for slot in SLOTS:
            entry = await self.raw(slot)
            if entry is None:
                continue
            slot_enum = SlotKind(entry["slot"])
            out.append({
                "slot": entry["slot"],
                "provider_id": entry["provider_id"],
                "status": entry["status"],
                "config": redact(slot_enum, entry["provider_id"], entry["config"]),
                "configured_fields": sorted(entry["config"].keys()),
                "last_test": entry["last_test"],
                "updated_at": entry["updated_at"],
            })
        return out

    # ── writes ───────────────────────────────────────────────────────
    async def save(
        self, slot: SlotKind, provider_id: str, config: dict[str, Any]
    ) -> dict[str, Any]:
        """Persist configuration, preserving secrets the user did not retype.

        The UI shows masked secrets. If the user edits an unrelated field and
        saves, the masked placeholder must not overwrite the real credential —
        so a value that still looks like the mask is dropped rather than stored.
        """
        assert self._pool is not None
        existing = await self.raw(slot)
        same_provider = bool(existing and existing["provider_id"] == provider_id)
        # Switching provider invalidates both the stored config and the test
        # result. Carrying a previous provider's test forward shows a stale
        # verdict against a connection that was never attempted.
        merged = dict(existing["config"]) if same_provider else {}

        p = provider(slot, provider_id)
        secrets = p.secret_fields() if p else set()
        for key, value in config.items():
            if key in secrets and isinstance(value, str) and value.startswith("••••"):
                continue  # untouched masked secret
            if value is None or value == "":
                merged.pop(key, None)
                continue
            merged[key] = value

        # A zero-config provider is connected on save; anything else must be
        # proven by a test before it counts as connected.
        status = (Status.CONNECTED.value if (p and p.zero_config)
                  else (existing["status"] if same_provider and existing
                        else Status.UNCONFIGURED.value))
        await self._pool.execute(
            """
            INSERT INTO plugin_config (slot, provider_id, config, status,
                                       last_test, updated_at)
            VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, now())
            ON CONFLICT (slot) DO UPDATE
              SET provider_id = EXCLUDED.provider_id,
                  config      = EXCLUDED.config,
                  status      = EXCLUDED.status,
                  last_test   = EXCLUDED.last_test,
                  updated_at  = now()
            """,
            slot.value, provider_id, json.dumps(merged), status,
            json.dumps(existing["last_test"]) if (same_provider and existing
                                                  and existing["last_test"]) else None,
        )
        return merged

    async def record_test(
        self, slot: SlotKind, ok: bool, detail: dict[str, Any]
    ) -> None:
        assert self._pool is not None
        payload = {
            "ok": ok,
            "at": datetime.now(timezone.utc).isoformat(),
            **detail,
        }
        await self._pool.execute(
            "UPDATE plugin_config SET status = $2, last_test = $3::jsonb, "
            "updated_at = now() WHERE slot = $1",
            slot.value,
            Status.CONNECTED.value if ok else Status.ERROR.value,
            json.dumps(payload),
        )
