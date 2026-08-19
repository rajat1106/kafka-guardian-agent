"""OPA client.

Every mutating action is evaluated against policy/guardian.rego before it
reaches an actuator. If OPA is unreachable the gate fails *closed* — an
unavailable policy engine means unknown blast radius, and executing an
action of unknown blast radius unattended is precisely what this layer
exists to prevent.
"""

from __future__ import annotations

import httpx
import structlog

from guardian_platform.config import ClusterCapabilities
from guardian_platform.contracts import Action, Diagnosis, PolicyDecision

log = structlog.get_logger(__name__)

_DECISION_PATH = "/v1/data/guardian/remediation/decision"


class PolicyGate:
    def __init__(self, opa_url: str, auto_approve_max_blast: int) -> None:
        self._url = opa_url.rstrip("/")
        self._max_blast = auto_approve_max_blast

    async def evaluate(
        self,
        action: Action,
        capabilities: ClusterCapabilities,
        diagnosis: Diagnosis | None,
        current_state: dict,
        failed_action_count: int = 0,
    ) -> PolicyDecision:
        payload = {
            "input": {
                "action": {
                    "type": action.type.value,
                    "target": action.target,
                    "params": action.params,
                    "reversible": action.reversible,
                },
                "capabilities": capabilities.model_dump(),
                "settings": {"auto_approve_max_blast": self._max_blast},
                "diagnosis": {"confidence": diagnosis.confidence if diagnosis else 0.0},
                "incident": {"failed_action_count": failed_action_count},
                "current": current_state,
            }
        }
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.post(f"{self._url}{_DECISION_PATH}", json=payload)
                resp.raise_for_status()
                result = resp.json().get("result")
        except Exception as exc:  # noqa: BLE001
            log.error("policy_unavailable", error=str(exc))
            return PolicyDecision(
                effect="deny",
                blast_radius=5,
                reasons=[
                    f"policy engine unreachable ({type(exc).__name__}); failing "
                    "closed because blast radius cannot be established"
                ],
                matched_rule="fail_closed",
            )

        if not result:
            return PolicyDecision(
                effect="deny", blast_radius=5,
                reasons=["policy returned no decision for this action"],
                matched_rule="undefined",
            )
        return PolicyDecision(**result)
