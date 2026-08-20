import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import {
  ArrowRight, Check, Clock, GitCommitHorizontal, History, ShieldAlert,
  Undo2, X,
} from "lucide-react";
import { api, type ApprovalContext, type ApprovalRequest } from "@/lib/api";
import { action as plainAction, ago, duration, REACH_BY_BLAST } from "@/lib/plain";

interface Props {
  request: ApprovalRequest;
  onDecide: (incidentId: string, planId: string, approved: boolean) => void;
}

/**
 * The decision surface.
 *
 * An earlier version showed the action, its blast radius and the policy's
 * reasons — which describes what the action *is* and says nothing about the
 * decision. The questions a person actually has are: what happens if I do
 * nothing, how long do I have, what did we do last time, and how often is the
 * agent right about this. Every one is answerable from data the system
 * already holds, so all four are here.
 */
export const ApprovalCard = ({ request, onDecide }: Props) => {
  const [ctx, setCtx] = useState<ApprovalContext | null>(null);

  useEffect(() => {
    let alive = true;
    api.approvalContext(request.incident_id)
      .then((c) => alive && setCtx(c))
      .catch(() => undefined);
    return () => { alive = false; };
  }, [request.incident_id]);

  const act = request.actions[0];
  const decision = request.decisions[0];
  if (!act || !decision) return null;

  const plain = plainAction(act.type);
  const severe = decision.blast_radius >= 4;
  const seconds = ctx?.time.seconds_until_breach ?? null;
  const urgent = seconds !== null && seconds < 120;
  const record = ctx?.track_record;

  return (
    <Card className={cn(
      "border-2",
      severe ? "border-rose-400/70" : "border-amber-400/60",
    )}>
      <CardHeader className="pb-3">
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          <ShieldAlert className={cn("h-5 w-5",
            severe ? "text-rose-400" : "text-amber-400")} />
          {plain.verb}
          <Badge variant="outline" className="ml-auto font-mono text-[10px]">
            blast {decision.blast_radius}/5
          </Badge>
        </CardTitle>
        <p className="text-xs text-muted-foreground">
          on <span className="text-foreground">{act.target}</span> ·{" "}
          {REACH_BY_BLAST[decision.blast_radius] ?? "unknown reach"}
        </p>
      </CardHeader>

      <CardContent className="space-y-3">
        {/* how long you have — the thing that decides whether to read on */}
        {seconds !== null && (
          <div className={cn(
            "flex items-center gap-2 rounded-lg px-3 py-2 text-sm",
            urgent ? "bg-rose-500/15 text-rose-300" : "bg-amber-500/10 text-amber-300",
          )}>
            <Clock className="h-4 w-4 shrink-0" />
            <span>
              <span className="font-semibold">{duration(seconds)}</span> until{" "}
              {ctx?.time.metric?.replace(/_/g, " ") ?? "the limit"} is breached
            </span>
          </div>
        )}

        {/* the two outcomes, side by side */}
        <div className="grid gap-2 sm:grid-cols-2">
          <div className="rounded-lg border border-emerald-400/25 bg-emerald-500/5 p-2.5">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-emerald-400">
              If you approve
            </div>
            <p className="mt-1 text-xs">{plain.detail}</p>
            <code className="mt-1.5 block break-all font-mono text-[10px] text-muted-foreground">
              {act.type}
              {Object.entries(act.params ?? {}).map(([k, v]) => ` --${k}=${v}`)}
            </code>
            {ctx?.undo ? (
              <p className="mt-1.5 flex items-center gap-1 text-[10px] text-emerald-400/80">
                <Undo2 className="h-3 w-3" /> can be undone automatically
              </p>
            ) : (
              <p className="mt-1.5 text-[10px] text-amber-400/80">
                cannot be undone automatically
              </p>
            )}
          </div>

          <div className="rounded-lg border border-border/60 bg-muted/25 p-2.5">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              If you do nothing
            </div>
            <p className="mt-1 text-xs text-muted-foreground">
              {ctx?.if_rejected ??
                "The agent takes no action and escalates the incident."}
            </p>
          </div>
        </div>

        {/* why the agent thinks this */}
        {ctx?.reasoning && (
          <p className="text-xs leading-relaxed text-muted-foreground">
            {ctx.reasoning}
          </p>
        )}

        {/* what changed — the first thing a human would check */}
        {ctx?.correlated_changes?.length ? (
          <div className="rounded-lg bg-muted/30 px-2.5 py-2">
            <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              <GitCommitHorizontal className="h-3 w-3" /> Recent changes
            </div>
            {ctx.correlated_changes.slice(0, 2).map((c, i) => (
              <p key={i} className="mt-0.5 text-[11px] text-muted-foreground">{c}</p>
            ))}
          </div>
        ) : null}

        {/* track record beats a confidence percentage */}
        {record && record.seen > 0 && (
          <div className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
            <History className="h-3 w-3 shrink-0" />
            <span>
              The agent has diagnosed this{" "}
              <span className="text-foreground">{record.seen}</span> time
              {record.seen === 1 ? "" : "s"} and resolved it{" "}
              <span className={cn(
                (record.rate ?? 0) >= 0.7 ? "text-emerald-400" : "text-amber-400",
              )}>
                {record.resolved}
              </span>
              .
            </span>
          </div>
        )}

        {/* what happened last time */}
        {ctx?.prior_incidents?.length ? (
          <div className="space-y-1">
            {ctx.prior_incidents.slice(0, 2).map((p) => (
              <Link
                key={p.incident_id}
                to={`/incidents/${p.incident_id}`}
                className="flex items-center gap-2 rounded bg-muted/25 px-2 py-1 text-[11px] transition-colors hover:bg-muted/50"
              >
                <span className={cn("h-1.5 w-1.5 shrink-0 rounded-full",
                  p.resolved ? "bg-emerald-400" : "bg-amber-400")} />
                <span className="truncate text-muted-foreground">
                  {ago(p.ts)}: {p.actions_taken.join(", ") || "no action"} —{" "}
                  {p.resolved ? `fixed in ${duration(p.mttr_seconds)}` : "did not resolve"}
                </span>
                <ArrowRight className="ml-auto h-3 w-3 shrink-0 opacity-50" />
              </Link>
            ))}
          </div>
        ) : null}

        {/* why policy stopped it */}
        <details className="text-[11px]">
          <summary className="cursor-pointer text-muted-foreground">
            Why this needs your approval
          </summary>
          <ul className="mt-1 ml-4 list-disc space-y-0.5 text-muted-foreground">
            {decision.reasons.map((r, i) => <li key={i}>{r}</li>)}
          </ul>
          <p className="mt-1 font-mono text-[10px] text-muted-foreground">
            rule {decision.matched_rule}
          </p>
        </details>

        <div className="flex gap-2 pt-0.5">
          <Button size="sm" className="flex-1"
                  onClick={() => onDecide(request.incident_id, request.plan_id, true)}>
            <Check className="mr-1 h-4 w-4" /> Approve
          </Button>
          <Button size="sm" variant="outline" className="flex-1"
                  onClick={() => onDecide(request.incident_id, request.plan_id, false)}>
            <X className="mr-1 h-4 w-4" /> Reject
          </Button>
          <Button asChild size="sm" variant="ghost">
            <Link to={`/incidents/${request.incident_id}`}>Details</Link>
          </Button>
        </div>
      </CardContent>
    </Card>
  );
};
