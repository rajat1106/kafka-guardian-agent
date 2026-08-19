import { useMemo } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import {
  ArrowRight, CheckCircle2, Clock, Coins, HeartPulse, ShieldAlert,
  Sparkles, TriangleAlert,
} from "lucide-react";

import { StatTile } from "@/components/StatTile";
import { ApprovalCard } from "@/components/ApprovalCard";
import { useGuardian } from "@/hooks/useGuardian";
import {
  ago, action as plainAction, cause as plainCause, confidenceWord,
  decisionSentence, duration, outcomeSentence,
} from "@/lib/plain";

/**
 * The page a non-engineer sees first.
 *
 * Same events as the operations view, described in terms of what happened to
 * the business rather than which metric crossed which threshold. Nothing is
 * simplified away — the detail is one click deeper, on Operations.
 */
const Overview = () => {
  const { state, cluster, connected, approve } = useGuardian();

  const services = Object.values(state.metrics);
  const unhealthy = services.filter((s) => !s.healthy);

  const resolved = state.outcomes.filter((o) => o.resolved);
  const autonomous = resolved.filter((o) => !o.human_approved);
  const escalated = state.outcomes.filter((o) => !o.resolved);
  const avgMttr = resolved.length
    ? resolved.reduce((s, o) => s + o.mttr_seconds, 0) / resolved.length
    : 0;

  // Time the agent bought by acting before something broke, taken straight
  // from the detector's own prediction rather than estimated.
  const preventedSeconds = useMemo(
    () =>
      state.anomalies
        .filter((a) => a.predicted_breach_seconds)
        .reduce((s, a) => s + (a.predicted_breach_seconds ?? 0), 0),
    [state.anomalies],
  );

  const spend = cluster?.guardian?.budget?.day_cost_usd ?? 0;

  const feed = useMemo(
    () => state.outcomes.slice(0, 8),
    [state.outcomes],
  );

  const statusLine = unhealthy.length
    ? `${unhealthy.length} of ${services.length} services need attention`
    : services.length
      ? `All ${services.length} services healthy`
      : "Waiting for the first readings";

  return (
    <div className="mx-auto max-w-[1600px] space-y-5 px-4 py-5 md:px-6">
      {/* headline */}
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">
            {unhealthy.length ? "Handling an issue" : "Everything is running"}
          </h1>
          <p className="mt-0.5 text-sm text-muted-foreground">
            {statusLine} · {cluster?.description ?? "connecting"} ·
            {" "}decisions made by{" "}
            <span className="text-foreground">{cluster?.guardian?.planner ?? "…"}</span>
          </p>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link to="/connections">
            Connect your own cluster <ArrowRight className="ml-1.5 h-4 w-4" />
          </Link>
        </Button>
      </div>

      {/* things needing a human come first */}
      {state.pending_approvals.length > 0 && (
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-sm font-medium text-amber-400">
            <ShieldAlert className="h-4 w-4" />
            The agent is holding {state.pending_approvals.length} action
            {state.pending_approvals.length > 1 ? "s" : ""} for your approval
          </div>
          <p className="text-xs text-muted-foreground">
            These reach far enough that the agent will not take them on its own.
          </p>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {state.pending_approvals.map((req) => (
              <ApprovalCard key={req.incident_id} request={req} onDecide={approve} />
            ))}
          </div>
        </div>
      )}

      {/* headline numbers */}
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <StatTile
          label="Fixed without anyone"
          value={String(autonomous.length)}
          sub={`of ${state.outcomes.length} issues handled`}
          icon={CheckCircle2}
          tone={autonomous.length ? "good" : "neutral"}
        />
        <StatTile
          label="Average time to fix"
          value={avgMttr ? duration(avgMttr) : "—"}
          sub="from detection to confirmed"
          icon={Clock}
        />
        <StatTile
          label="Warning time bought"
          value={preventedSeconds ? duration(preventedSeconds) : "—"}
          sub="caught before anything broke"
          icon={HeartPulse}
          tone={preventedSeconds ? "good" : "neutral"}
        />
        <StatTile
          label="Raised to a person"
          value={String(escalated.length + state.pending_approvals.length)}
          sub="agent was not confident enough"
          icon={TriangleAlert}
          tone={escalated.length ? "warn" : "neutral"}
        />
        <StatTile
          label="Model spend today"
          value={`$${spend.toFixed(2)}`}
          sub={cluster?.guardian?.budget
            ? `capped at $${((cluster.guardian.budget.day_limit / 1000) * 0.006).toFixed(2)}`
            : "no model configured"}
          icon={Coins}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-3">
        {/* what happened, in sentences */}
        <Card className="lg:col-span-2">
          <CardHeader className="pb-3">
            <CardTitle className="text-base">What the agent has been doing</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5">
            {feed.length === 0 && (
              <p className="text-sm text-muted-foreground">
                Nothing has happened yet. The agent is watching.
              </p>
            )}
            {feed.map((o) => {
              const c = plainCause(o.root_cause);
              return (
                <div
                  key={o.outcome_id}
                  className={cn(
                    "rounded-lg border-l-2 bg-muted/25 py-2.5 pl-3 pr-3",
                    o.resolved ? "border-l-emerald-400" : "border-l-amber-400",
                  )}
                >
                  <div className="flex flex-wrap items-baseline gap-x-2">
                    <span className="text-sm font-medium">{c.title}</span>
                    <span className="text-xs text-muted-foreground">
                      on {o.service.replace("-service", "")}
                    </span>
                    <span className="ml-auto text-xs text-muted-foreground">
                      {ago(o.ts)}
                    </span>
                  </div>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {outcomeSentence(o)}
                  </p>
                  {!o.resolved && (
                    <p className="mt-1 text-xs text-amber-400/90">
                      Risk if ignored: {c.risk}
                    </p>
                  )}
                </div>
              );
            })}
          </CardContent>
        </Card>

        {/* how it decides */}
        <div className="space-y-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex items-center gap-2 text-base">
                <Sparkles className="h-4 w-4 text-primary" />
                How it decides
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              <p className="text-muted-foreground">
                The agent acts on its own only when an action is small and
                reversible. Everything with wider reach waits for you.
              </p>
              <div className="space-y-1.5">
                {[
                  ["Adds capacity, resizes a pool", "on its own", "good"],
                  ["Restarts a service", "asks you first", "warn"],
                  ["Restarts a Kafka server", "asks you first", "warn"],
                  ["Moves an entire region", "asks you first", "bad"],
                ].map(([what, who, tone]) => (
                  <div key={what as string}
                       className="flex items-center justify-between gap-2 rounded bg-muted/30 px-2.5 py-1.5">
                    <span className="text-xs">{what}</span>
                    <Badge variant="outline" className={cn("shrink-0 text-[10px]",
                      tone === "good" ? "border-emerald-400/40 text-emerald-400"
                        : tone === "warn" ? "border-amber-400/40 text-amber-400"
                          : "border-rose-400/40 text-rose-400")}>
                      {who}
                    </Badge>
                  </div>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">
                These rules are enforced by a policy engine, not by the model —
                the agent cannot talk its way past them.
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Latest decision</CardTitle>
            </CardHeader>
            <CardContent>
              {state.actions[0] ? (
                <div className="space-y-2 text-sm">
                  <div className="font-medium">
                    {plainAction(state.actions[0].action.type).verb}
                  </div>
                  <p className="text-muted-foreground">
                    {plainAction(state.actions[0].action.type).detail}
                  </p>
                  <div className="flex flex-wrap items-center gap-2 pt-1">
                    <Badge variant="outline" className="text-[10px]">
                      {decisionSentence(state.actions[0])}
                    </Badge>
                    <span className="text-xs text-muted-foreground">
                      {plainAction(state.actions[0].action.type).reach}
                    </span>
                  </div>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">
                  No decisions yet.
                </p>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default Overview;
