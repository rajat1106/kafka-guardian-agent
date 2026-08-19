/**
 * Plain-language translation.
 *
 * The operations view speaks in σ, blast radius and root-cause slugs, which
 * is right for the person on call and wrong for everyone else. Rather than
 * building a second, weaker dashboard, the same events are rendered through
 * this layer: what happened, what it would have cost, what was done.
 *
 * Nothing here invents information. Every phrase is a restatement of a field
 * that exists on the event.
 */

import type { ActionResult, Anomaly, Diagnosis, Outcome } from "./api";

/** What the agent believed was wrong, in a sentence a non-engineer can act on. */
export const CAUSE_PLAIN: Record<string, { title: string; meaning: string; risk: string }> = {
  consumer_capacity_shortfall: {
    title: "Falling behind on work",
    meaning:
      "Messages were arriving faster than the service could process them, so a backlog was building up.",
    risk: "Left alone, the backlog grows until the service runs out of memory and stops.",
  },
  db_connection_pool_exhaustion: {
    title: "Ran out of database connections",
    meaning:
      "The service uses a fixed pool of database connections and had nearly used all of them.",
    risk: "Requests start queueing, response times spike, and customers see errors.",
  },
  memory_leak_suspected: {
    title: "Memory climbing with no backlog",
    meaning:
      "Memory use was rising even though there was no work queued up — the signature of a leak rather than overload.",
    risk: "The service eventually exhausts its memory and is killed.",
  },
  region_unavailable: {
    title: "A region went down",
    meaning: "The service stopped responding entirely in its data-centre region.",
    risk: "Complete loss of service for anything depending on it.",
  },
  partition_replication_degraded: {
    title: "Data copies fell out of sync",
    meaning:
      "Kafka keeps redundant copies of data. Some copies stopped keeping up, so the safety margin was gone.",
    risk: "Throughput looks fine, but a single further failure could lose data.",
  },
  service_degradation: {
    title: "Service slower than usual",
    meaning: "Response times and errors rose without a clear cause in the available data.",
    risk: "Unclear — this one needs a human to look.",
  },
  unclassified_anomaly: {
    title: "Something unusual",
    meaning: "Readings drifted from normal but matched no known failure pattern.",
    risk: "Unknown. The agent deliberately took no action.",
  },
};

/** What the agent did about it. */
export const ACTION_PLAIN: Record<string, { verb: string; detail: string; reach: string }> = {
  scale_consumer_group: {
    verb: "Added processing capacity",
    detail: "Started more workers so the backlog could be worked off.",
    reach: "Affects one service, and can be undone",
  },
  increase_partitions: {
    verb: "Raised the parallelism limit",
    detail: "Increased how many workers can process this data stream at once.",
    reach: "Affects one data stream — permanent, cannot be undone",
  },
  adjust_db_pool: {
    verb: "Enlarged the connection pool",
    detail: "Gave the service more database connections and recycled the stuck ones.",
    reach: "Affects one service briefly, and can be undone",
  },
  restart_service: {
    verb: "Restarted the service",
    detail: "A clean restart to reclaim memory.",
    reach: "Brief outage for one service",
  },
  clear_service_backlog: {
    verb: "Discarded the backlog",
    detail: "Dropped queued messages to let the service catch up.",
    reach: "Loses data — a last resort",
  },
  throttle_producer: {
    verb: "Slowed the incoming rate",
    detail: "Reduced how fast work arrives so the service could recover.",
    reach: "Slows everything sending to this service",
  },
  reset_consumer_offset: {
    verb: "Skipped ahead past the backlog",
    detail: "Moved to the newest messages instead of working through old ones.",
    reach: "Skips unprocessed data",
  },
  roll_broker: {
    verb: "Restarted a Kafka server",
    detail: "Recycled a broker so data copies could resynchronise.",
    reach: "Affects every stream on that server",
  },
  failover_region: {
    verb: "Moved to another region",
    detail: "Switched traffic to a healthy data centre.",
    reach: "Affects an entire region — the largest action available",
  },
  alter_topic_config: {
    verb: "Changed stream settings",
    detail: "Adjusted configuration on the data stream.",
    reach: "Affects one data stream",
  },
  no_op: {
    verb: "Took no action",
    detail: "The agent was not confident enough to act, so it escalated instead.",
    reach: "Nothing changed",
  },
};

export const REACH_BY_BLAST: Record<number, string> = {
  0: "Nothing changed",
  1: "One service, reversible",
  2: "One service and what talks to it",
  3: "Brief disruption to one service",
  4: "A shared Kafka server",
  5: "An entire region",
};

export const METRIC_PLAIN: Record<string, string> = {
  consumer_lag: "work backlog",
  memory_used_pct: "memory use",
  p99_latency_ms: "response time",
  error_rate: "error rate",
  cpu_pct: "CPU use",
  db_pool_utilisation: "database connections in use",
  under_replicated_partitions: "out-of-sync data copies",
  healthy: "service availability",
  multivariate: "overall behaviour",
};

export const cause = (key: string) =>
  CAUSE_PLAIN[key] ?? {
    title: key.replace(/_/g, " "),
    meaning: "An issue the agent identified.",
    risk: "Unknown.",
  };

export const action = (key: string) =>
  ACTION_PLAIN[key] ?? {
    verb: key.replace(/_/g, " "),
    detail: "",
    reach: "Unknown reach",
  };

export const metric = (key: string) => METRIC_PLAIN[key] ?? key.replace(/_/g, " ");

/** "2 minutes ago" — relative time without pulling in a date library. */
export function ago(iso: string): string {
  const seconds = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (seconds < 45) return "just now";
  if (seconds < 90) return "a minute ago";
  if (seconds < 3600) return `${Math.round(seconds / 60)} minutes ago`;
  if (seconds < 7200) return "an hour ago";
  if (seconds < 86400) return `${Math.round(seconds / 3600)} hours ago`;
  return `${Math.round(seconds / 86400)} days ago`;
}

export function duration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)} seconds`;
  const mins = seconds / 60;
  if (mins < 60) return `${mins.toFixed(mins < 10 ? 1 : 0)} minutes`;
  return `${(mins / 60).toFixed(1)} hours`;
}

/** One sentence describing a closed incident. */
export function outcomeSentence(o: Outcome): string {
  const c = cause(o.root_cause);
  const service = o.service.replace("-service", "");
  if (!o.actions_taken.length || o.actions_taken.every((a) => a === "no_op")) {
    return `${c.title} on ${service}. The agent was not confident enough to act and raised it for a person.`;
  }
  const acts = o.actions_taken.map((a) => action(a).verb.toLowerCase());
  const verb = acts.join(" and ");
  const closing = o.resolved
    ? `Confirmed fixed in ${duration(o.mttr_seconds)}.`
    : `It did not resolve, so it was escalated.`;
  const who = o.human_approved ? " after a person approved it" : "";
  return `${c.title} on ${service}. The agent ${verb}${who}. ${closing}`;
}

/** How an anomaly reads before diagnosis. */
export function anomalySentence(a: Anomaly): string {
  const service = a.service.replace("-service", "");
  const m = metric(a.metric);
  if (a.predicted_breach_seconds) {
    return `${service}: ${m} is rising and will hit its limit in about ${Math.round(
      a.predicted_breach_seconds,
    )} seconds.`;
  }
  if (a.metric === "healthy") return `${service} stopped responding.`;
  return `${service}: ${m} is unusually high.`;
}

export function confidenceWord(c: number): string {
  if (c >= 0.85) return "very confident";
  if (c >= 0.7) return "confident";
  if (c >= 0.5) return "fairly confident";
  return "not confident";
}

export function diagnosisSentence(d: Diagnosis): string {
  const c = cause(d.root_cause);
  return `${c.title} — ${c.meaning}`;
}

/** Whether an action was allowed, held, or refused, in plain terms. */
export function decisionSentence(r: ActionResult): string {
  if (r.decision.effect === "deny") return "Refused by the safety rules";
  if (r.decision.effect === "require_approval")
    return r.executed ? "Approved by a person" : "Waiting for a person";
  return "Done automatically";
}
