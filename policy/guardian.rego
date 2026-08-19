# Guardian remediation policy.
#
# Every mutating action the agent wants to take is evaluated here before it
# reaches an actuator. The policy answers two questions:
#
#   1. What is this action's blast radius (0-5)?
#   2. Given that radius, the cluster's capabilities and the agent's
#      confidence, may it proceed unattended?
#
# The default is deny. An action type with no rule here cannot be executed,
# which means adding a capability to the agent requires a deliberate policy
# change rather than merely a new tool function.

package guardian.remediation

import rego.v1

# ── Blast radius ────────────────────────────────────────────────────────
# 0-1  reversible, scoped to one service
# 2    reversible, affects one service's clients
# 3    disruptive to one service
# 4    affects a shared resource (a broker serves every topic)
# 5    affects an entire region

# A lookup table rather than a rule per type: an unknown action must still
# produce a decision object, and it must produce the *most* cautious one.
# With per-type rules, an unrecognised type leaves blast_radius undefined,
# which makes the whole decision undefined — the agent then sees no denial
# at all, which is the opposite of failing safe.
radius_table := {
    "no_op": 0,
    "scale_consumer_group": 1,
    "clear_service_backlog": 1,
    "adjust_db_pool": 2,
    "increase_partitions": 2,
    "alter_topic_config": 2,
    "throttle_producer": 3,
    "restart_service": 3,
    "reset_consumer_offset": 3,
    "roll_broker": 4,
    "failover_region": 5,
}

known_action if { radius_table[input.action.type] }

blast_radius := radius_table[input.action.type]

# Unknown action types get the maximum radius, and hard_deny rejects them.
default blast_radius := 5

# ── Capability gate ─────────────────────────────────────────────────────
# Some actions are impossible on a managed cluster. Denying them here — with
# an explanation — is better than letting the actuator fail at execution
# time, because the agent gets the reason back and can re-plan.

capability_violation contains msg if {
    input.action.type == "roll_broker"
    not input.capabilities.can_restart_broker
    msg := "broker restart is unavailable on a managed cluster (Confluent Cloud); escalate to a human or use a topic-level remedy"
}

capability_violation contains msg if {
    input.action.type == "alter_topic_config"
    not input.capabilities.can_alter_broker_config
    input.action.params.scope == "broker"
    msg := "broker-level configuration cannot be altered on a managed cluster"
}

capability_violation contains msg if {
    input.action.type == "increase_partitions"
    not input.capabilities.can_increase_partitions
    msg := "partition increase is not permitted on this cluster"
}

capability_violation contains msg if {
    input.action.type == "reset_consumer_offset"
    not input.capabilities.can_reset_consumer_offsets
    msg := "consumer offset reset is not permitted on this cluster"
}

# ── Hard denials ────────────────────────────────────────────────────────
# These hold regardless of confidence or operator settings.

hard_deny contains msg if {
    not known_action
    msg := sprintf("unknown action type %q has no policy entry; denied by default", [input.action.type])
}

hard_deny contains msg if {
    some msg in capability_violation
}

hard_deny contains msg if {
    input.action.type == "increase_partitions"
    input.action.params.partitions <= input.current.partition_count
    msg := "partition count can only increase; requested value is not greater than current"
}

hard_deny contains msg if {
    input.action.type == "scale_consumer_group"
    input.action.params.replicas > 20
    msg := "replica count above the hard ceiling of 20"
}

hard_deny contains msg if {
    # More consumers than partitions do no work in a Kafka consumer group.
    # Allowing this would let the agent "fix" lag with idle pods forever.
    input.action.type == "scale_consumer_group"
    input.action.params.replicas > input.current.partition_count
    msg := sprintf("cannot scale to %d consumers: the topic has only %d partitions, so the extra consumers would sit idle — increase partitions first", [input.action.params.replicas, input.current.partition_count])
}

hard_deny contains msg if {
    input.action.type == "reset_consumer_offset"
    input.action.params.to == "earliest"
    msg := "resetting to earliest replays the entire retention window and is never an incident remedy"
}

# ── Approval requirements ───────────────────────────────────────────────

needs_approval contains msg if {
    blast_radius > input.settings.auto_approve_max_blast
    msg := sprintf("blast radius %d exceeds the unattended ceiling of %d", [blast_radius, input.settings.auto_approve_max_blast])
}

needs_approval contains msg if {
    input.diagnosis.confidence < 0.7
    blast_radius >= 2
    msg := sprintf("diagnosis confidence %.2f is below 0.70 for a blast-radius-%d action", [input.diagnosis.confidence, blast_radius])
}

needs_approval contains msg if {
    not input.action.reversible
    msg := "action is not reversible"
}

needs_approval contains msg if {
    # An agent that has already failed twice on this incident is not
    # converging; a human should look before it tries something bigger.
    input.incident.failed_action_count >= 2
    msg := "two or more actions have already failed on this incident"
}

# ── Decision ────────────────────────────────────────────────────────────

default effect := "deny"

effect := "deny" if { count(hard_deny) > 0 }

effect := "require_approval" if {
    count(hard_deny) == 0
    count(needs_approval) > 0
}

effect := "allow" if {
    count(hard_deny) == 0
    count(needs_approval) == 0
    known_action
}

reasons := array.concat(
    [m | some m in hard_deny],
    [m | some m in needs_approval],
)

matched_rule := sprintf("%s/blast_%d", [input.action.type, blast_radius])

decision := {
    "effect": effect,
    "blast_radius": blast_radius,
    "reasons": reasons,
    "matched_rule": matched_rule,
}
