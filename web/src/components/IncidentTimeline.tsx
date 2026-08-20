import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
  AlertTriangle, Brain, CheckCircle2, ShieldAlert, ShieldX, Terminal, XCircle,
} from "lucide-react";
import type { TimelineEntry } from "@/lib/api";
import {
  action as plainAction, cause as plainCause, confidenceWord, duration,
  metric as plainMetric,
} from "@/lib/plain";

const KIND_META: Record<string, { label: string; icon: typeof Brain; tone: string }> = {
  detected: { label: "Detected", icon: AlertTriangle, tone: "text-amber-400" },
  diagnosed: { label: "Diagnosed", icon: Brain, tone: "text-primary" },
  held_for_approval: { label: "Held for approval", icon: ShieldAlert, tone: "text-amber-400" },
  acted: { label: "Acted", icon: Terminal, tone: "text-sky-400" },
  closed: { label: "Closed", icon: CheckCircle2, tone: "text-emerald-400" },
};

/**
 * The full story of one incident.
 *
 * The important part is the "acted" step: it shows the exact operation the
 * agent performed, its parameters, what the policy engine decided about it
 * and why, and what the actuator reported back. That is the difference
 * between trusting the agent and auditing it.
 */
export const TimelineStep = ({ entry }: { entry: TimelineEntry }) => {
  const meta = KIND_META[entry.kind] ?? KIND_META.detected;
  const Icon = meta.icon;
  const p = entry.payload as Record<string, never>;

  return (
    <div className="relative pl-7">
      <span className="absolute left-0 top-0.5">
        <Icon className={cn("h-4 w-4", meta.tone)} />
      </span>
      <div className="flex flex-wrap items-baseline gap-x-2">
        <span className="text-sm font-medium">{meta.label}</span>
        {entry.ts && (
          <span className="text-[11px] text-muted-foreground">
            {new Date(entry.ts).toLocaleTimeString()}
          </span>
        )}
      </div>

      {entry.kind === "detected" && (
        <p className="mt-0.5 text-xs text-muted-foreground">
          {plainMetric(String(p.metric))} was abnormal —{" "}
          <span className="font-mono">{String(p.description)}</span>
        </p>
      )}

      {entry.kind === "diagnosed" && (
        <div className="mt-0.5 space-y-1">
          <p className="text-xs">
            <span className="font-medium">{plainCause(String(p.root_cause)).title}</span>
            <span className="text-muted-foreground">
              {" "}— {confidenceWord(Number(p.confidence))} (
              {Math.round(Number(p.confidence) * 100)}%)
            </span>
          </p>
          <p className="text-xs leading-relaxed text-muted-foreground">
            {String(p.reasoning)}
          </p>
          <p className="text-[11px] text-muted-foreground">
            decided by{" "}
            <span className="font-mono">
              {p.source === "llm" ? String(p.model) : "built-in rules engine"}
            </span>
            {Number(p.tokens_used) > 0 &&
              ` · ${Number(p.tokens_used)} tokens · $${Number(p.cost_usd).toFixed(5)}`}
          </p>
        </div>
      )}

      {entry.kind === "held_for_approval" && (
        <p className="mt-0.5 text-xs text-muted-foreground">
          Policy would not let the agent proceed unattended.
        </p>
      )}

      {/* the audit-grade step */}
      {entry.kind === "acted" && (() => {
        const act = p.action as unknown as {
          type: string; target: string; params: Record<string, unknown>;
          rationale: string; reversible: boolean;
        };
        const dec = p.decision as unknown as {
          effect: string; blast_radius: number; reasons: string[]; matched_rule: string;
        };
        const executed = Boolean(p.executed);
        const success = Boolean(p.success);
        const args = Object.entries(act.params ?? {})
          .map(([k, v]) => `--${k}=${JSON.stringify(v)}`)
          .join(" ");
        return (
          <div className="mt-1 space-y-2">
            <p className="text-xs">{plainAction(act.type).verb}</p>

            {/* what actually ran */}
            <div className="rounded-lg border border-border/60 bg-black/40 px-2.5 py-2">
              <div className="mb-1 flex items-center gap-1.5 text-[10px] uppercase tracking-wider text-muted-foreground">
                <Terminal className="h-3 w-3" />
                operation
              </div>
              <code className="block break-all font-mono text-[11px] text-sky-300">
                {act.type} --target={act.target}{args ? ` ${args}` : ""}
              </code>
              <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                <Badge variant="outline" className="text-[9px]">
                  via {String(p.actuator)}
                </Badge>
                <Badge variant="outline" className="text-[9px]">
                  {act.reversible ? "reversible" : "irreversible"}
                </Badge>
                <Badge variant="outline" className="text-[9px]">
                  blast {dec.blast_radius}/5
                </Badge>
                {Number(p.duration_ms) > 0 && (
                  <span className="text-[10px] text-muted-foreground">
                    {Number(p.duration_ms)} ms
                  </span>
                )}
              </div>
            </div>

            {/* what the policy said */}
            <div className={cn(
              "rounded-lg px-2.5 py-2 text-[11px]",
              dec.effect === "deny" ? "bg-rose-500/10 text-rose-300"
                : dec.effect === "require_approval" ? "bg-amber-500/10 text-amber-300"
                  : "bg-emerald-500/10 text-emerald-300",
            )}>
              <div className="flex items-center gap-1.5 font-medium">
                {dec.effect === "deny" ? <ShieldX className="h-3 w-3" />
                  : <ShieldAlert className="h-3 w-3" />}
                Policy: {dec.effect.replace("_", " ")}
                <span className="ml-auto font-mono opacity-70">{dec.matched_rule}</span>
              </div>
              {dec.reasons?.length > 0 && (
                <ul className="mt-1 list-disc space-y-0.5 pl-4 opacity-90">
                  {dec.reasons.map((r, i) => <li key={i}>{r}</li>)}
                </ul>
              )}
            </div>

            {/* what came back */}
            <div className="flex items-start gap-1.5 text-xs">
              {executed
                ? (success ? <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-emerald-400" />
                           : <XCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-rose-400" />)
                : <ShieldX className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />}
              <span className="text-muted-foreground">{String(p.detail)}</span>
            </div>

            {act.rationale && (
              <p className="text-[11px] italic text-muted-foreground">
                Why: {act.rationale}
              </p>
            )}
          </div>
        );
      })()}

      {entry.kind === "closed" && (
        <div className="mt-0.5 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <Badge className={cn("text-[10px]", p.resolved
              ? "bg-emerald-500/20 text-emerald-300"
              : "bg-amber-500/20 text-amber-300")}>
              {p.resolved ? "resolved" : "escalated"}
            </Badge>
            <span className="text-xs text-muted-foreground">
              took {duration(Number(p.mttr_seconds))}
            </span>
            {Boolean(p.human_approved) && (
              <Badge variant="outline" className="text-[10px]">a person approved</Badge>
            )}
          </div>
          <p className="text-xs text-muted-foreground">
            Verification: {String(p.verification_detail)}
          </p>
        </div>
      )}
    </div>
  );
};

