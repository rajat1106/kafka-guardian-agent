import { useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";
import { Activity, AlertTriangle, Bot, FlaskConical, Timer, Zap } from "lucide-react";
import { cn } from "@/lib/utils";

import { MetricCard } from "@/components/MetricCard";
import { ApprovalCard } from "@/components/ApprovalCard";
import { AgentReasoning } from "@/components/AgentReasoning";
import { ActionLog } from "@/components/ActionLog";
import { ClusterBanner } from "@/components/ClusterBanner";
import { useGuardian } from "@/hooks/useGuardian";
import type { Diagnosis } from "@/lib/api";

const DETECTOR_LABEL: Record<string, string> = {
  zscore: "z-score",
  isolation_forest: "isolation forest",
  trend_forecast: "trend forecast",
  threshold: "threshold",
};

const Index = () => {
  const { state, cluster, scenarios, connected, error, inject, approve, toggleChaos } =
    useGuardian();
  const [chaosOn, setChaosOn] = useState(true);
  const [focus, setFocus] = useState<string | null>(null);

  const services = useMemo(() => Object.values(state.metrics), [state.metrics]);
  const selected = focus ?? services[0]?.service ?? null;

  const latestDiagnosis = useMemo(() => {
    const d = state.decisions.find(
      (x): x is Diagnosis => "root_cause" in x && typeof x.root_cause === "string",
    );
    return d ?? null;
  }, [state.decisions]);

  const resolved = state.outcomes.filter((o) => o.resolved).length;
  const autoResolved = state.outcomes.filter((o) => o.resolved && !o.human_approved).length;
  const avgMttr =
    state.outcomes.length > 0
      ? state.outcomes.reduce((s, o) => s + o.mttr_seconds, 0) / state.outcomes.length
      : 0;

  const unhealthy = services.filter((s) => !s.healthy).length;

  const chartData = useMemo(
    () => (selected ? (state.series[selected] ?? []) : []).map((p, i) => ({ ...p, i })),
    [state.series, selected],
  );

  return (
    <div className="min-h-screen bg-background p-4 md:p-6">
      <div className="mx-auto max-w-[1600px] space-y-4">
        {/* header */}
        <div className="flex flex-wrap items-center gap-3">
          <Bot className="h-7 w-7 text-primary" />
          <div>
            <h1 className="text-xl font-bold tracking-tight">Kafka Guardian Agent</h1>
            <p className="text-xs text-muted-foreground">
              Autonomous detection, diagnosis and policy-gated remediation
            </p>
          </div>
          <div className="ml-auto flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              onClick={async () => setChaosOn(await toggleChaos())}
            >
              <FlaskConical className="mr-1.5 h-4 w-4" />
              auto-chaos {chaosOn ? "on" : "off"}
            </Button>
          </div>
        </div>

        <ClusterBanner cluster={cluster} connected={connected} />

        {error && (
          <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
            {error}
          </div>
        )}

        {/* approvals — the human gate, first because it blocks recovery */}
        {state.pending_approvals.length > 0 && (
          <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
            {state.pending_approvals.map((req) => (
              <ApprovalCard key={req.incident_id} request={req} onDecide={approve} />
            ))}
          </div>
        )}

        {/* summary */}
        <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
          <MetricCard
            title="Services healthy"
            value={`${services.length - unhealthy}/${services.length || 0}`}
            status={unhealthy > 0 ? "critical" : "healthy"}
          />
          <MetricCard
            title="Incidents resolved"
            value={`${resolved}/${state.outcomes.length}`}
            status={resolved === state.outcomes.length ? "healthy" : "warning"}
            change={`${autoResolved} without a human`}
          />
          <MetricCard
            title="Mean time to resolve"
            value={avgMttr > 0 ? avgMttr.toFixed(0) : "—"}
            unit="s"
            status="info"
          />
          <MetricCard
            title="Awaiting approval"
            value={String(state.pending_approvals.length)}
            status={state.pending_approvals.length > 0 ? "warning" : "healthy"}
          />
        </div>

        <div className="grid gap-4 lg:grid-cols-3">
          {/* left: telemetry */}
          <div className="space-y-4 lg:col-span-2">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="flex flex-wrap items-center gap-2 text-base">
                  <Activity className="h-5 w-5 text-primary" />
                  Live telemetry
                  <div className="ml-auto flex flex-wrap gap-1">
                    {services.map((s) => (
                      <Button
                        key={s.service}
                        size="sm"
                        variant={selected === s.service ? "default" : "ghost"}
                        className="h-7 text-xs"
                        onClick={() => setFocus(s.service)}
                      >
                        <span
                          className={cn(
                            "mr-1.5 h-1.5 w-1.5 rounded-full",
                            s.healthy ? "bg-success" : "bg-destructive",
                          )}
                        />
                        {s.service.replace("-service", "")}
                      </Button>
                    ))}
                  </div>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ResponsiveContainer width="100%" height={200}>
                  <AreaChart data={chartData}>
                    <defs>
                      <linearGradient id="lag" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(var(--primary))" stopOpacity={0.5} />
                        <stop offset="95%" stopColor="hsl(var(--primary))" stopOpacity={0} />
                      </linearGradient>
                      <linearGradient id="lat" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="hsl(var(--warning))" stopOpacity={0.5} />
                        <stop offset="95%" stopColor="hsl(var(--warning))" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" opacity={0.15} />
                    <XAxis dataKey="i" hide />
                    <YAxis yAxisId="l" tick={{ fontSize: 10 }} width={45} />
                    <YAxis yAxisId="r" orientation="right" tick={{ fontSize: 10 }} width={45} />
                    <Tooltip
                      contentStyle={{
                        background: "hsl(var(--card))",
                        border: "1px solid hsl(var(--border))",
                        borderRadius: 8,
                        fontSize: 12,
                      }}
                    />
                    <Area
                      yAxisId="l" type="monotone" dataKey="consumer_lag" name="consumer lag"
                      stroke="hsl(var(--primary))" fill="url(#lag)" strokeWidth={2}
                      isAnimationActive={false}
                    />
                    <Area
                      yAxisId="r" type="monotone" dataKey="p99_latency_ms" name="p99 (ms)"
                      stroke="hsl(var(--warning))" fill="url(#lat)" strokeWidth={2}
                      isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>

                {selected && state.metrics[selected] && (
                  <div className="mt-3 grid grid-cols-2 gap-2 text-xs md:grid-cols-4">
                    {[
                      ["heap", `${state.metrics[selected].memory_used_pct.toFixed(0)}%`],
                      ["pool", `${state.metrics[selected].db_pool_used}/${state.metrics[selected].db_pool_size}`],
                      ["replicas", String(state.metrics[selected].partition_count)],
                      ["errors", `${(state.metrics[selected].error_rate * 100).toFixed(1)}%`],
                    ].map(([k, v]) => (
                      <div key={k} className="rounded bg-muted/40 px-2 py-1.5">
                        <div className="text-muted-foreground">{k}</div>
                        <div className="font-mono font-medium">{v}</div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>

            <Tabs defaultValue="actions">
              <TabsList>
                <TabsTrigger value="actions">Audit trail</TabsTrigger>
                <TabsTrigger value="anomalies">Anomalies</TabsTrigger>
                <TabsTrigger value="outcomes">Outcomes</TabsTrigger>
              </TabsList>

              <TabsContent value="actions" className="mt-3">
                <ActionLog actions={state.actions} />
              </TabsContent>

              <TabsContent value="anomalies" className="mt-3">
                <Card>
                  <CardContent className="pt-4">
                    <ScrollArea className="h-[320px] pr-3">
                      {state.anomalies.length === 0 && (
                        <p className="text-sm text-muted-foreground">
                          No anomalies. The detector is watching.
                        </p>
                      )}
                      <div className="space-y-2">
                        {state.anomalies.map((a) => (
                          <div
                            key={a.anomaly_id}
                            className="rounded-lg border border-border/50 p-2.5"
                          >
                            <div className="flex flex-wrap items-center gap-2">
                              <AlertTriangle
                                className={cn(
                                  "h-4 w-4",
                                  a.severity === "critical" ? "text-destructive" : "text-warning",
                                )}
                              />
                              <code className="font-mono text-xs font-semibold">{a.metric}</code>
                              <span className="text-xs text-muted-foreground">{a.service}</span>
                              <Badge variant="outline" className="ml-auto text-[10px]">
                                {DETECTOR_LABEL[a.detector] ?? a.detector}
                              </Badge>
                            </div>
                            <p className="mt-1 pl-6 text-xs text-muted-foreground">
                              {a.description}
                            </p>
                            {a.predicted_breach_seconds !== null && (
                              <p className="mt-1 flex items-center gap-1 pl-6 text-[11px] text-warning">
                                <Timer className="h-3 w-3" />
                                predicted breach in {a.predicted_breach_seconds.toFixed(0)}s
                              </p>
                            )}
                          </div>
                        ))}
                      </div>
                    </ScrollArea>
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="outcomes" className="mt-3">
                <Card>
                  <CardContent className="pt-4">
                    <ScrollArea className="h-[320px] pr-3">
                      {state.outcomes.length === 0 && (
                        <p className="text-sm text-muted-foreground">No closed incidents yet.</p>
                      )}
                      <div className="space-y-2">
                        {state.outcomes.map((o) => (
                          <div
                            key={o.outcome_id}
                            className="rounded-lg border border-border/50 p-2.5"
                          >
                            <div className="flex flex-wrap items-center gap-2">
                              <Badge
                                className={cn(
                                  "text-[10px]",
                                  o.resolved
                                    ? "bg-success/20 text-success"
                                    : "bg-warning/20 text-warning",
                                )}
                              >
                                {o.resolved ? "resolved" : "escalated"}
                              </Badge>
                              <code className="font-mono text-xs">{o.root_cause}</code>
                              <span className="ml-auto font-mono text-[10px] text-muted-foreground">
                                {o.mttr_seconds.toFixed(0)}s
                                {o.human_approved && " · human approved"}
                              </span>
                            </div>
                            <p className="mt-1 text-xs text-muted-foreground">
                              {o.verification_detail}
                            </p>
                          </div>
                        ))}
                      </div>
                    </ScrollArea>
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </div>

          {/* right: agent + chaos */}
          <div className="space-y-4">
            <AgentReasoning
              diagnosis={latestDiagnosis}
              planner={cluster?.guardian?.planner ?? "…"}
            />

            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="flex items-center gap-2 text-base">
                  <Zap className="h-5 w-5 text-warning" />
                  Break something
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {scenarios.map((s) => (
                  <button
                    key={s.key}
                    onClick={() => inject(s.key)}
                    className="w-full rounded-lg border border-border/50 p-2.5 text-left transition-colors hover:border-primary/50 hover:bg-muted/40"
                  >
                    <div className="text-sm font-medium">{s.title}</div>
                    <div className="mt-0.5 text-xs text-muted-foreground">{s.service}</div>
                    <div className="mt-1 font-mono text-[10px] text-muted-foreground">
                      expects: {s.ground_truth}
                    </div>
                  </button>
                ))}
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Index;
