"""Service-level actions against the demo fleet's control API."""

from __future__ import annotations

import os
import time

import httpx
import structlog

from guardian_platform.contracts import Action, ActionType

from .base import Actuator, ExecutionResult

log = structlog.get_logger(__name__)

_OWNED = {
    ActionType.SCALE_CONSUMER_GROUP,
    ActionType.RESTART_SERVICE,
    ActionType.ADJUST_DB_POOL,
    ActionType.CLEAR_SERVICE_BACKLOG,
    ActionType.THROTTLE_PRODUCER,
    ActionType.FAILOVER_REGION,
    ActionType.ROLL_BROKER,
    ActionType.NO_OP,
}


class SimulatedActuator(Actuator):
    name = "simulated"

    def __init__(self, fleet_url: str | None = None) -> None:
        self._url = (fleet_url or os.getenv("FLEET_URL", "http://fleet:8081")).rstrip("/")

    def handles(self, action_type: ActionType) -> bool:
        return action_type in _OWNED

    async def execute(self, action: Action) -> ExecutionResult:
        started = time.perf_counter()
        try:
            detail = await self._dispatch(action)
            ok = True
        except httpx.HTTPStatusError as exc:
            detail, ok = f"fleet rejected the action: {exc.response.text}", False
        except Exception as exc:  # noqa: BLE001
            detail, ok = f"{type(exc).__name__}: {exc}", False
        return ExecutionResult(
            ok, detail, self.name, int((time.perf_counter() - started) * 1000)
        )

    async def _dispatch(self, action: Action) -> str:
        svc = action.target
        async with httpx.AsyncClient(timeout=10) as c:
            if action.type is ActionType.NO_OP:
                return "no action taken"

            if action.type is ActionType.SCALE_CONSUMER_GROUP:
                r = await c.post(f"{self._url}/services/{svc}/scale",
                                 json={"replicas": int(action.params["replicas"])})
                r.raise_for_status()
                d = r.json()
                return (f"scaled {svc} consumers from {d['replicas_before']} to "
                        f"{d['replicas_after']}")

            if action.type is ActionType.ADJUST_DB_POOL:
                r = await c.post(f"{self._url}/services/{svc}/pool",
                                 json={"size": int(action.params["size"])})
                r.raise_for_status()
                d = r.json()
                return (f"resized {svc} connection pool from {d['pool_before']} to "
                        f"{d['pool_after']} (recycling leaked connections)")

            if action.type is ActionType.RESTART_SERVICE:
                r = await c.post(f"{self._url}/services/{svc}/restart")
                r.raise_for_status()
                return f"restarted {svc}; it will be unavailable for ~4s"

            if action.type is ActionType.CLEAR_SERVICE_BACKLOG:
                r = await c.post(f"{self._url}/services/{svc}/clear-backlog")
                r.raise_for_status()
                d = r.json()
                return f"dropped {svc} backlog from {d['lag_before']} to {d['lag_after']}"

            if action.type is ActionType.THROTTLE_PRODUCER:
                factor = float(action.params.get("factor", 0.6))
                r = await c.post(f"{self._url}/services/{svc}/throttle",
                                 json={"factor": factor})
                r.raise_for_status()
                return f"throttled producers into {svc} to {factor:.0%} of nominal rate"

            if action.type is ActionType.FAILOVER_REGION:
                target_region = action.params.get("to", "us-west-2")
                r = await c.post(f"{self._url}/services/{svc}/heal")
                r.raise_for_status()
                return f"failed {svc} over to {target_region}; health restored"

            if action.type is ActionType.ROLL_BROKER:
                r = await c.post(f"{self._url}/services/{svc}/heal")
                r.raise_for_status()
                return f"rolled broker serving {svc}; replicas resynchronised"

        return f"unhandled action {action.type.value}"
