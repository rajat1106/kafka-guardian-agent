package guardian.remediation_test

import data.guardian.remediation
import rego.v1

# ── fixtures ────────────────────────────────────────────────────────────

self_hosted := {
    "can_restart_broker": true, "can_alter_broker_config": true,
    "can_create_topics": true, "can_increase_partitions": true,
    "can_reassign_partitions": true, "can_reset_consumer_offsets": true,
    "can_delete_topics": true, "min_replication_factor": 1,
}

confluent := {
    "can_restart_broker": false, "can_alter_broker_config": false,
    "can_create_topics": true, "can_increase_partitions": true,
    "can_reassign_partitions": false, "can_reset_consumer_offsets": true,
    "can_delete_topics": true, "min_replication_factor": 3,
}

base(action, caps) := {
    "action": action,
    "capabilities": caps,
    "settings": {"auto_approve_max_blast": 2},
    "diagnosis": {"confidence": 0.9},
    "incident": {"failed_action_count": 0},
    "current": {"partition_count": 6, "replicas": 2},
}

scale(n) := {"type": "scale_consumer_group", "target": "payment-service",
             "params": {"replicas": n}, "reversible": true}

# ── low blast radius auto-executes ──────────────────────────────────────

test_scale_within_partitions_is_allowed if {
    d := remediation.decision with input as base(scale(4), self_hosted)
    d.effect == "allow"
    d.blast_radius == 1
}

# ── the Kafka-specific trap ─────────────────────────────────────────────

test_scale_beyond_partition_count_is_denied if {
    d := remediation.decision with input as base(scale(9), self_hosted)
    d.effect == "deny"
    count([r | some r in d.reasons; contains(r, "sit idle")]) == 1
}

# ── blast radius drives approval ────────────────────────────────────────

test_restart_requires_approval if {
    a := {"type": "restart_service", "target": "user-service", "params": {}, "reversible": true}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "require_approval"
    d.blast_radius == 3
}

test_failover_requires_approval if {
    a := {"type": "failover_region", "target": "notification-service",
          "params": {"to": "us-west-2"}, "reversible": true}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "require_approval"
    d.blast_radius == 5
}

# ── capability gate: same action, different cluster ─────────────────────

test_roll_broker_allowed_intent_on_self_hosted if {
    a := {"type": "roll_broker", "target": "broker-1", "params": {}, "reversible": true}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "require_approval"   # radius 4, not denied
}

test_roll_broker_denied_on_confluent if {
    a := {"type": "roll_broker", "target": "broker-1", "params": {}, "reversible": true}
    d := remediation.decision with input as base(a, confluent)
    d.effect == "deny"
    count([r | some r in d.reasons; contains(r, "managed cluster")]) > 0
}

# ── confidence gate ─────────────────────────────────────────────────────

test_low_confidence_escalates_medium_blast if {
    inp := object.union(base({"type": "adjust_db_pool", "target": "user-service",
                              "params": {"size": 60}, "reversible": true}, self_hosted),
                        {"diagnosis": {"confidence": 0.55}})
    d := remediation.decision with input as inp
    d.effect == "require_approval"
}

test_high_confidence_allows_medium_blast if {
    a := {"type": "adjust_db_pool", "target": "user-service",
          "params": {"size": 60}, "reversible": true}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "allow"
}

# ── repeated failure escalates ──────────────────────────────────────────

test_repeated_failure_forces_approval if {
    inp := object.union(base(scale(4), self_hosted),
                        {"incident": {"failed_action_count": 2}})
    d := remediation.decision with input as inp
    d.effect == "require_approval"
}

# ── fail-safe defaults ──────────────────────────────────────────────────

test_unknown_action_is_denied_not_undefined if {
    a := {"type": "rm_minus_rf_slash", "target": "everything", "params": {}, "reversible": false}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "deny"
    d.blast_radius == 5
}

test_irreversible_action_requires_approval if {
    a := {"type": "clear_service_backlog", "target": "payment-service",
          "params": {}, "reversible": false}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "require_approval"
}

test_offset_reset_to_earliest_is_denied if {
    a := {"type": "reset_consumer_offset", "target": "payment-processors",
          "params": {"to": "earliest"}, "reversible": true}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "deny"
}

test_partition_decrease_is_denied if {
    a := {"type": "increase_partitions", "target": "payment-events",
          "params": {"partitions": 3}, "reversible": true}
    d := remediation.decision with input as base(a, self_hosted)
    d.effect == "deny"
}
