/**
 * Guardian API client.
 *
 * Replaces the previous mock client, which returned hardcoded arrays and
 * re-polled itself on a timer. Everything here is live: state arrives over a
 * WebSocket fed by the Kafka topics, and the only write path is the approval
 * endpoint, which publishes onto `guardian.approvals` rather than calling the
 * agent directly.
 */

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8080";
const WS_URL = import.meta.env.VITE_WS_URL ?? "ws://localhost:8080/ws";

export type Severity = "info" | "warning" | "critical";
export type PolicyEffect = "allow" | "require_approval" | "deny";

export interface ServiceMetrics {
  event_id: string;
  ts: string;
  service: string;
  consumer_lag: number;
  messages_per_sec: number;
  partition_count: number;
  under_replicated_partitions: number;
  memory_used_pct: number;
  cpu_pct: number;
  heap_used_mb: number;
  db_pool_used: number;
  db_pool_size: number;
  db_pool_utilisation: number;
  p99_latency_ms: number;
  error_rate: number;
  request_rate: number;
  healthy: boolean;
  region: string;
}

export interface Anomaly {
  anomaly_id: string;
  ts: string;
  service: string;
  metric: string;
  value: number;
  baseline: number;
  deviation_sigma: number;
  severity: Severity;
  score: number;
  detector: "zscore" | "isolation_forest" | "trend_forecast" | "threshold";
  predicted_breach_seconds: number | null;
  description: string;
}

export interface Diagnosis {
  diagnosis_id: string;
  incident_id: string;
  ts: string;
  root_cause: string;
  confidence: number;
  reasoning: string;
  evidence: string[];
  similar_incidents: string[];
  source: "llm" | "offline";
  model: string | null;
  tokens_used: number;
  cost_usd: number;
  latency_ms: number;
}

export interface PolicyDecision {
  effect: PolicyEffect;
  blast_radius: number;
  reasons: string[];
  matched_rule: string;
}

export interface Action {
  action_id: string;
  type: string;
  target: string;
  params: Record<string, unknown>;
  rationale: string;
  reversible: boolean;
  blast_radius: number | null;
}

export interface ActionResult {
  result_id: string;
  ts: string;
  incident_id: string;
  action: Action;
  decision: PolicyDecision;
  executed: boolean;
  success: boolean;
  detail: string;
  actuator: string;
  duration_ms: number;
}

export interface ApprovalRequest {
  incident_id: string;
  plan_id: string;
  actions: Action[];
  decisions: PolicyDecision[];
  requested_at: string;
}

export interface Outcome {
  outcome_id: string;
  ts: string;
  incident_id: string;
  service: string;
  resolved: boolean;
  root_cause: string;
  actions_taken: string[];
  mttr_seconds: number;
  verification_detail: string;
  tokens_used: number;
  cost_usd: number;
  human_approved: boolean;
}

export interface SeriesPoint {
  ts: string;
  consumer_lag: number;
  memory_used_pct: number;
  p99_latency_ms: number;
  error_rate: number;
  cpu_pct: number;
  db_pool_utilisation: number;
}

export interface GuardianState {
  metrics: Record<string, ServiceMetrics>;
  series: Record<string, SeriesPoint[]>;
  anomalies: Anomaly[];
  decisions: (Diagnosis | ApprovalRequest)[];
  actions: ActionResult[];
  outcomes: Outcome[];
  pending_approvals: ApprovalRequest[];
}

export interface ClusterInfo {
  provider: "local" | "confluent";
  description: string;
  bootstrap: string;
  security_protocol: string;
  topic_prefix: string;
  replication_factor: number;
  capabilities: Record<string, boolean | number | string>;
  guardian: {
    open_incidents?: { incident_id: string; service: string; state: string }[];
    budget?: {
      hour_used: number;
      hour_limit: number;
      day_used: number;
      day_limit: number;
      day_cost_usd: number;
      llm_calls_24h: number;
      min_severity: number;
    };
    planner?: string;
    memory?: Record<string, number>;
  };
}

export interface ScenarioInfo {
  key: string;
  title: string;
  service: string;
  description: string;
  ground_truth: string;
  acceptable_actions: string[];
}


export interface LineageNode {
  id: string;
  kind: "producer" | "topic" | "consumer";
  label: string;
  active?: boolean;
  // topic
  partitions?: number;
  replication_factor?: number;
  messages_per_sec?: number;
  under_replicated?: number;
  // consumer group
  group_id?: string;
  replicas?: number;
  lag?: number;
  healthy?: boolean;
  memory_used_pct?: number;
  p99_latency_ms?: number;
  error_rate?: number;
  db_pool_used?: number;
  db_pool_size?: number;
  region?: string;
  active_fault?: string | null;
  awaiting_approval?: boolean;
  recent_action?: {
    type: string;
    success: boolean;
    executed: boolean;
    blast_radius: number | null;
    ts: string;
  } | null;
}

export interface LineageEdge {
  id: string;
  source: string;
  target: string;
  rate?: number;
  lag?: number;
  healthy: boolean;
}

export interface LineageGraph {
  nodes: LineageNode[];
  edges: LineageEdge[];
  cluster?: string;
  provider?: string;
}

export const emptyState = (): GuardianState => ({
  metrics: {},
  series: {},
  anomalies: [],
  decisions: [],
  actions: [],
  outcomes: [],
  pending_approvals: [],
});

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${API_URL}${path}`);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json() as Promise<T>;
}

export const api = {
  state: () => get<GuardianState>("/api/state"),
  cluster: () => get<ClusterInfo>("/api/cluster"),
  scenarios: () => get<{ scenarios: ScenarioInfo[] }>("/api/scenarios"),
  services: () => get<Record<string, unknown>>("/api/services"),
  lineage: () => get<LineageGraph>("/api/lineage"),

  inject: async (key: string) => {
    const res = await fetch(`${API_URL}/api/scenarios/${key}/inject`, { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },

  toggleChaos: async () => {
    const res = await fetch(`${API_URL}/api/chaos/toggle`, { method: "POST" });
    return res.json() as Promise<{ enabled: boolean }>;
  },

  approve: async (incidentId: string, planId: string, approved: boolean) => {
    const res = await fetch(`${API_URL}/api/approve`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        incident_id: incidentId,
        plan_id: planId,
        approved,
        approved_by: "operator",
      }),
    });
    if (!res.ok) throw new Error(await res.text());
    return res.json() as Promise<{ published: boolean; was_pending: boolean }>;
  },
};

export type WsMessage =
  | { type: "snapshot"; data: GuardianState }
  | { type: "metrics"; data: ServiceMetrics }
  | { type: "anomaly"; data: Anomaly }
  | { type: "decision"; data: Diagnosis | ApprovalRequest }
  | { type: "action"; data: ActionResult }
  | { type: "outcome"; data: Outcome }
  | { type: "approval"; data: { incident_id: string } }
  | { type: "approval_expired"; data: { incident_id: string } };

/** Auto-reconnecting WebSocket. */
export function connect(
  onMessage: (msg: WsMessage) => void,
  onStatus: (connected: boolean) => void,
): () => void {
  let socket: WebSocket | null = null;
  let retry = 0;
  let closed = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let heartbeat: ReturnType<typeof setInterval> | undefined;

  const open = () => {
    if (closed) return;
    socket = new WebSocket(WS_URL);

    socket.onopen = () => {
      retry = 0;
      onStatus(true);
      // The server reads to detect disconnects; this keeps the socket warm
      // through intermediaries that close idle connections.
      heartbeat = setInterval(() => socket?.readyState === 1 && socket.send("ping"), 25_000);
    };
    socket.onmessage = (event) => {
      try {
        onMessage(JSON.parse(event.data) as WsMessage);
      } catch {
        /* ignore malformed frames rather than tearing down the socket */
      }
    };
    socket.onclose = () => {
      onStatus(false);
      if (heartbeat) clearInterval(heartbeat);
      if (closed) return;
      // Exponential backoff, capped — a dashboard left open overnight
      // should not hammer a restarting API.
      const delay = Math.min(1000 * 2 ** retry++, 15_000);
      timer = setTimeout(open, delay);
    };
    socket.onerror = () => socket?.close();
  };

  open();
  return () => {
    closed = true;
    if (timer) clearTimeout(timer);
    if (heartbeat) clearInterval(heartbeat);
    socket?.close();
  };
}
