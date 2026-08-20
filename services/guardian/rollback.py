"""Computing the inverse of a remediation.

Every mutating action needs a declared way back. The inverse must be built
*before* execution, because the state it depends on — the replica count to
return to, the pool size that was there — is gone afterwards.

Some actions have no inverse, and saying so explicitly matters more than
inventing one:

* `increase_partitions` is genuinely irreversible in Kafka. Partitions cannot
  be removed, and the key-to-partition mapping has already changed.
* `restart_service` cannot be un-restarted; the process is already gone.
* `clear_service_backlog` discarded data that no longer exists.

For those, `None` is the honest answer, and the policy layer already treats
irreversibility as grounds for requiring human approval — which is exactly
why that rule exists.
"""

from __future__ import annotations

from guardian_platform.contracts import Action, ActionType

# Actions whose effect cannot be undone by any later action.
IRREVERSIBLE = {
    ActionType.INCREASE_PARTITIONS,
    ActionType.RESTART_SERVICE,
    ActionType.CLEAR_SERVICE_BACKLOG,
    ActionType.RESET_CONSUMER_OFFSET,
    ActionType.ROLL_BROKER,
    ActionType.NO_OP,
}


def inverse_of(action: Action, current: dict) -> Action | None:
    """Build the action that undoes `action`, given state before it ran."""
    if action.type in IRREVERSIBLE:
        return None

    latest = current.get("latest") or {}

    if action.type is ActionType.SCALE_CONSUMER_GROUP:
        previous = current.get("replicas")
        if previous is None:
            return None
        return Action(
            type=ActionType.SCALE_CONSUMER_GROUP, target=action.target,
            params={"replicas": int(previous)},
            rationale=f"Restore consumer count to {previous}.",
            reversible=True,
        )

    if action.type is ActionType.ADJUST_DB_POOL:
        previous = current.get("db_pool_size")
        if previous is None:
            return None
        return Action(
            type=ActionType.ADJUST_DB_POOL, target=action.target,
            params={"size": int(previous)},
            rationale=f"Restore connection pool to {previous}.",
            reversible=True,
        )

    if action.type is ActionType.THROTTLE_PRODUCER:
        return Action(
            type=ActionType.THROTTLE_PRODUCER, target=action.target,
            params={"factor": 1.0},
            rationale="Remove the producer throttle.",
            reversible=True,
        )

    if action.type is ActionType.FAILOVER_REGION:
        previous = latest.get("region") or current.get("region")
        if not previous:
            return None
        return Action(
            type=ActionType.FAILOVER_REGION, target=action.target,
            params={"to": previous},
            rationale=f"Fail back to {previous}.",
            reversible=True,
        )

    if action.type is ActionType.ALTER_TOPIC_CONFIG:
        # Undoing a config change needs the prior values, which the planner
        # does not currently capture. Returning None is better than a
        # plausible-looking inverse that restores the wrong settings.
        return None

    return None


def describe(action: Action | None) -> str:
    if action is None:
        return "no automatic undo available"
    params = ", ".join(f"{k}={v}" for k, v in action.params.items())
    return f"{action.type.value}({params})"
