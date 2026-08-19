"""Reconciles the agent's brain with whatever the Connections page saved.

The decision engine is configured at runtime, not at boot, so the agent has
to pick up a key added minutes ago without a restart. This polls the plugin
store and swaps the planner in place when the stored configuration changes.

Only a slot the user has actually tested is honoured. An untested key is a
key that has never been proven to work, and discovering that during an
incident is the wrong time.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import asyncpg
import structlog

from guardian_platform.config import GuardianSettings

from budget import TokenBudget
from planner_llm import LLMPlanner

log = structlog.get_logger(__name__)

POLL_SECONDS = 15.0


def _fingerprint(entry: dict[str, Any] | None) -> str:
    """Stable identity for a configuration, so we only rebuild on real change."""
    if not entry:
        return "none"
    cfg = entry.get("config") or {}
    key = cfg.get("api_key") or ""
    return "|".join([
        entry.get("provider_id", ""),
        entry.get("status", ""),
        cfg.get("model_triage", ""),
        cfg.get("model_diagnose", ""),
        str(cfg.get("tokens_per_day", "")),
        # The key itself is never logged; a short digest is enough to notice
        # it changed.
        str(hash(key) & 0xFFFF) if key else "",
    ])


class BrainSupervisor:
    """Keeps `apply(planner, settings)` in step with the stored brain config."""

    def __init__(
        self,
        dsn: str,
        base_settings: GuardianSettings,
        budget: TokenBudget,
        apply: Callable[[LLMPlanner | None, str], None],
    ) -> None:
        self._dsn = dsn
        self._base = base_settings
        self._budget = budget
        self._apply = apply
        self._pool: asyncpg.Pool | None = None
        self._last = "__init__"

    async def start(self) -> None:
        for attempt in range(30):
            try:
                self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=2)
                break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(2)
        if self._pool is None:
            log.warning("brain_supervisor_no_db", note="staying on the rules engine")
            return
        await self._reconcile()

    async def _read(self) -> dict[str, Any] | None:
        if self._pool is None:
            return None
        try:
            row = await self._pool.fetchrow(
                "SELECT provider_id, config, status FROM plugin_config WHERE slot='brain'"
            )
        except asyncpg.UndefinedTableError:
            # The API service owns this table and may not have created it yet.
            return None
        except Exception as exc:  # noqa: BLE001
            log.debug("brain_config_read_failed", error=str(exc))
            return None
        if row is None:
            return None
        cfg = row["config"]
        return {
            "provider_id": row["provider_id"],
            "status": row["status"],
            "config": json.loads(cfg) if isinstance(cfg, str) else (cfg or {}),
        }

    async def _reconcile(self) -> None:
        entry = await self._read()
        fingerprint = _fingerprint(entry)
        if fingerprint == self._last:
            return
        self._last = fingerprint

        if not entry or entry["provider_id"] != "anthropic":
            self._apply(None, "rules engine")
            log.info("brain_selected", provider=(entry or {}).get("provider_id", "offline"))
            return

        if entry["status"] != "connected":
            # Configured but never proven. Refuse it rather than find out
            # mid-incident that the key is wrong.
            self._apply(None, "rules engine (LLM configured but not tested)")
            log.info("brain_untested", note="run the connection test to enable it")
            return

        cfg = entry["config"]
        key = (cfg.get("api_key") or "").strip()
        if not key:
            self._apply(None, "rules engine (no API key)")
            return

        settings = self._base.model_copy(update={
            "model_triage": cfg.get("model_triage") or self._base.model_triage,
            "model_diagnose": cfg.get("model_diagnose") or self._base.model_diagnose,
        })
        if cfg.get("tokens_per_day"):
            try:
                self._budget.per_day = int(cfg["tokens_per_day"])
            except (TypeError, ValueError):
                pass

        planner = LLMPlanner(key, settings, self._budget)
        self._apply(planner, f"{settings.model_triage} → {settings.model_diagnose}")
        log.info("brain_selected", provider="anthropic",
                 triage=settings.model_triage, diagnose=settings.model_diagnose,
                 tokens_per_day=self._budget.per_day)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(POLL_SECONDS)
            try:
                await self._reconcile()
            except Exception as exc:  # noqa: BLE001
                log.warning("brain_reconcile_failed", error=str(exc))

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
