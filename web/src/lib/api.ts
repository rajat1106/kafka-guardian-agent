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
/** Standard Kafka console (kafbat/kafka-ui) for raw topic and message browsing. */
export const KAFKA_UI_URL =
  import.meta.env.VITE_KAFKA_UI_URL ?? "http://localhost:8090";

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
  /** Present when the graph could not be built — surfaced, not swallowed. */
  error?: string;
}


/* ── plugins ─────────────────────────────────────────────────────── */

export interface FieldSpec {
  name: string;
  label: string;
  type: "text" | "password" | "select" | "number" | "boolean" | "textarea";
  required: boolean;
  secret: boolean;
  placeholder: string;
  help: string;
  default: string | number | boolean | null;
  options: { value: string; label: string }[];
}

export interface ProviderSpec {
  id: string;
  slot: string;
  name: string;
  summary: string;
  notes: string;
  zero_config: boolean;
  recommended: boolean;
  fields: FieldSpec[];
}

export interface SlotSpec {
  slot: string;
  title: string;
  description: string;
  required: boolean;
  default: string;
  providers: ProviderSpec[];
}

export interface PluginState {
  slot: string;
  provider_id: string;
  status: "unconfigured" | "testing" | "connected" | "error";
  /** Secrets arrive masked ("••••1234") and are never the real value. */
  config: Record<string, string | number | boolean>;
  configured_fields: string[];
  last_test: Record<string, unknown> | null;
  updated_at: string | null;
}

export interface DemoContainer {
  service: string;
  name: string;
  status: string;
  health: string | null;
  running: boolean;
  image: string | null;
}

export interface PluginsResponse {
  catalogue: SlotSpec[];
  configured: PluginState[];
  demo: {
    available: boolean;
    error?: string;
    containers: DemoContainer[];
    running: number;
    total: number;
    all_up: boolean;
  };
  plugins?: PluginState[];
}

export interface TimelineEntry {
  kind: "detected" | "diagnosed" | "held_for_approval" | "acted" | "closed";
  ts: string;
  payload: Record<string, unknown>;
}

export interface IncidentDetail {
  incident_id: string;
  service: string | null;
  outcome: Outcome | null;
  diagnosis: Diagnosis | null;
  approval: ApprovalRequest | null;
  actions: ActionResult[];
  anomalies: Anomaly[];
  timeline: TimelineEntry[];
}

export interface ApprovalContext {
  incident_id: string;
  service: string | null;
  root_cause: string | null;
  confidence: number | null;
  reasoning: string;
  correlated_changes: string[];
  action_type: string | null;
  undo: { type: string; params: Record<string, unknown> } | null;
  time: { seconds_until_breach: number | null; metric: string | null };
  track_record: { seen: number; resolved: number; rate: number | null };
  prior_incidents: {
    incident_id: string; ts: string; resolved: boolean;
    actions_taken: string[]; mttr_seconds: number;
    verification_detail: string; human_approved: boolean;
  }[];
  if_rejected: string;
}

export interface ChangeEvent {
  change_id: string;
  ts: string;
  kind: string;
  service: string;
  summary: string;
  reference: string;
  author: string;
  version: string;
  source: string;
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

/**
 * All requests carry a timeout. Without one, a hung API leaves a button
 * spinning forever with no way for the user to tell the difference between
 * "still working" and "never coming back".
 */
async function req(path: string, init: RequestInit = {}, timeoutMs = 20_000) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(`${API_URL}${path}`, { ...init, signal: ctrl.signal });
  } catch (e) {
    if ((e as Error).name === "AbortError") {
      throw new Error(`Timed out after ${timeoutMs / 1000}s — is the API running?`);
    }
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

async function get<T>(path: string): Promise<T> {
  const res = await req(path);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json() as Promise<T>;
}

export const api = {
  state: () => get<GuardianState>("/api/state"),
  cluster: () => get<ClusterInfo>("/api/cluster"),
  scenarios: () => get<{ scenarios: ScenarioInfo[] }>("/api/scenarios"),
  services: () => get<Record<string, unknown>>("/api/services"),
  lineage: () => get<LineageGraph>("/api/lineage"),
  incident: (id: string) => get<IncidentDetail>(`/api/incidents/${id}`),
  approvalContext: (id: string) =>
    get<ApprovalContext>(`/api/approvals/${id}/context`),
  changes: () => get<{ changes: ChangeEvent[] }>("/api/changes"),
  shadowReport: () => get<{
    total: number; would_have_auto_executed: number;
    would_have_needed_a_human: number; automation_rate: number;
    by_root_cause: Record<string, number>;
  }>("/api/shadow/report"),
  autonomy: () => get<{ mode: string; modes: { value: string; label: string; description: string }[] }>("/api/autonomy"),

  inject: async (key: string) => {
    const res = await req(`/api/scenarios/${key}/inject`, { method: "POST" });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },

  toggleChaos: async () => {
    const res = await req("/api/chaos/toggle", { method: "POST" });
    return res.json() as Promise<{ enabled: boolean }>;
  },


  // ── plugins ──────────────────────────────────────────────────────
  plugins: () => get<PluginsResponse>("/api/plugins"),

  configurePlugin: async (
    slot: string,
    providerId: string,
    config: Record<string, string>,
  ) => {
    const res = await req(`/api/plugins/${slot}/configure`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ provider_id: providerId, config }),
    });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  },

  testPlugin: async (slot: string) => {
    // A Kafka connection test can legitimately take 25s before it gives up.
    const res = await req(`/api/plugins/${slot}/test`, { method: "POST" }, 60_000);
    if (!res.ok) throw new Error(await res.text());
    return res.json() as Promise<Record<string, unknown>>;
  },

  discoverSource: () => get<Record<string, unknown>>("/api/plugins/source/discover"),

  // ── demo cluster ─────────────────────────────────────────────────
  demoScenarios: () => get<{
    simulated: { key: string; title: string; service: string; description: string }[];
    infrastructure: { key: string; title: string; description: string; danger: string }[];
  }>("/api/demo/scenarios"),

  demoStart: async () => {
    const r = await req("/api/demo/start", { method: "POST" }, 120_000);
    return r.json();
  },
  demoRecover: async () => {
    const r = await req("/api/demo/recover", { method: "POST" }, 180_000);
    return r.json();
  },
  demoStop: async () => {
    const r = await req("/api/demo/stop", { method: "POST" }, 120_000);
    return r.json();
  },
  injectInfra: async (key: string) => {
    const r = await req(`/api/demo/infra/${key}/inject`, { method: "POST" }, 60_000);
    return r.json();
  },
  recoverInfra: async (key: string) => {
    const r = await req(`/api/demo/infra/${key}/recover`, { method: "POST" }, 60_000);
    return r.json();
  },

  approve: async (incidentId: string, planId: string, approved: boolean) => {
    const res = await req("/api/approve", {
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
