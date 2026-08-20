import { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "sonner";
import {
  api, connect, emptyState,
  type ApprovalRequest, type ClusterInfo, type GuardianState,
  type ScenarioInfo, type WsMessage,
} from "@/lib/api";

const SERIES_CAP = 90;
const LIST_CAP = 100;

const capped = <T,>(list: T[], item: T, cap = LIST_CAP) => [item, ...list].slice(0, cap);

export interface UseGuardian {
  state: GuardianState;
  cluster: ClusterInfo | null;
  scenarios: ScenarioInfo[];
  connected: boolean;
  error: string | null;
  inject: (key: string) => Promise<void>;
  approve: (incidentId: string, planId: string, approved: boolean) => Promise<void>;
  toggleChaos: () => Promise<boolean>;
}

export function useGuardian(): UseGuardian {
  const [state, setState] = useState<GuardianState>(emptyState);
  const [cluster, setCluster] = useState<ClusterInfo | null>(null);
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([]);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const mounted = useRef(true);

  const apply = useCallback((msg: WsMessage) => {
    setState((prev) => {
      switch (msg.type) {
        case "snapshot":
          return msg.data;

        case "metrics": {
          const m = msg.data;
          const series = prev.series[m.service] ?? [];
          return {
            ...prev,
            metrics: { ...prev.metrics, [m.service]: m },
            series: {
              ...prev.series,
              [m.service]: [
                ...series,
                {
                  ts: m.ts,
                  consumer_lag: m.consumer_lag,
                  memory_used_pct: m.memory_used_pct,
                  p99_latency_ms: m.p99_latency_ms,
                  error_rate: m.error_rate,
                  cpu_pct: m.cpu_pct,
                  db_pool_utilisation: m.db_pool_utilisation,
                },
              ].slice(-SERIES_CAP),
            },
          };
        }

        case "anomaly":
          return { ...prev, anomalies: capped(prev.anomalies, msg.data) };

        case "decision": {
          const d = msg.data as ApprovalRequest;
          // The decisions topic carries both diagnoses and approval requests;
          // an approval request is the shape that has decisions attached.
          const isApproval = "decisions" in d && Array.isArray(d.decisions);
          return {
            ...prev,
            decisions: capped(prev.decisions, msg.data, 60),
            pending_approvals: isApproval
              ? [d, ...prev.pending_approvals.filter((p) => p.incident_id !== d.incident_id)]
              : prev.pending_approvals,
          };
        }

        case "action":
          return {
            ...prev,
            actions: capped(prev.actions, msg.data),
            pending_approvals: prev.pending_approvals.filter(
              (p) => p.incident_id !== msg.data.incident_id,
            ),
          };

        case "outcome":
          return {
            ...prev,
            outcomes: capped(prev.outcomes, msg.data, 60),
            pending_approvals: prev.pending_approvals.filter(
              (p) => p.incident_id !== msg.data.incident_id,
            ),
          };

        case "approval":
        case "approval_expired":
          return {
            ...prev,
            pending_approvals: prev.pending_approvals.filter(
              (p) => p.incident_id !== msg.data.incident_id,
            ),
          };

        default:
          return prev;
      }
    });
  }, []);

  useEffect(() => {
    mounted.current = true;

    api.state().then((s) => mounted.current && setState(s)).catch((e) => setError(String(e)));
    api.scenarios()
      .then((r) => mounted.current && setScenarios(r.scenarios))
      .catch(() => undefined);

    const refreshCluster = () =>
      api.cluster().then((c) => mounted.current && setCluster(c)).catch(() => undefined);
    refreshCluster();
    // Cluster info includes the agent's live token budget, which changes as
    // incidents are handled — poll it rather than only reading it once.
    const clusterTimer = setInterval(refreshCluster, 10_000);

    const disconnect = connect(apply, setConnected);
    return () => {
      mounted.current = false;
      clearInterval(clusterTimer);
      disconnect();
    };
  }, [apply]);

  const inject = useCallback(async (key: string) => {
    try {
      await api.inject(key);
      setError(null);
      toast.success(`Injected ${key.replace(/_/g, " ")}`, {
        description: "The agent should react within about 20 seconds.",
      });
    } catch (e) {
      const msg = (e as Error).message || String(e);
      setError(msg);
      toast.error(`Could not inject ${key}`, { description: msg });
    }
  }, []);

  const approve = useCallback(
    async (incidentId: string, planId: string, approved: boolean) => {
      // Every outcome is announced. A decision this consequential must never
      // be ambiguous about whether it registered — the previous version
      // swallowed a 401 and left the button looking broken.
      try {
        const res = await api.approve(incidentId, planId, approved);
        if (!res.was_pending) {
          const msg = "That request had already expired or been answered.";
          setError(msg);
          toast.warning(msg);
          return;
        }
        setError(null);
        toast.success(
          approved ? "Approved — the agent is acting now" : "Rejected",
          { description: approved
              ? "Watch the audit trail for the result."
              : "The incident has been escalated for a person to handle." },
        );
      } catch (e) {
        const msg = (e as Error).message || String(e);
        setError(msg);
        toast.error("Could not record your decision", { description: msg });
      }
    },
    [],
  );

  const toggleChaos = useCallback(async () => {
    const res = await api.toggleChaos();
    return res.enabled;
  }, []);

  return { state, cluster, scenarios, connected, error, inject, approve, toggleChaos };
}
