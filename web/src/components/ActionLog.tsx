import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import { CheckCircle2, XCircle, ShieldX, ScrollText } from "lucide-react";
import type { ActionResult } from "@/lib/api";

const effectStyle: Record<string, string> = {
  allow: "bg-success/15 text-success border-success/30",
  require_approval: "bg-warning/15 text-warning border-warning/30",
  deny: "bg-destructive/15 text-destructive border-destructive/30",
};

/** Append-only audit trail: every action with the policy decision that gated it. */
export const ActionLog = ({
  actions,
  onOpen,
}: {
  actions: ActionResult[];
  onOpen?: (incidentId: string) => void;
}) => (
  <Card>
    <CardHeader className="pb-3">
      <CardTitle className="flex items-center gap-2 text-base">
        <ScrollText className="h-5 w-5 text-muted-foreground" />
        Action audit trail
        <Badge variant="outline" className="ml-auto text-xs">{actions.length}</Badge>
      </CardTitle>
    </CardHeader>
    <CardContent>
      <ScrollArea className="h-[320px] pr-3">
        {actions.length === 0 && (
          <p className="text-sm text-muted-foreground">No actions taken yet.</p>
        )}
        <div className="space-y-2">
          {actions.map((a) => {
            const Icon = !a.executed ? ShieldX : a.success ? CheckCircle2 : XCircle;
            const colour = !a.executed
              ? "text-muted-foreground"
              : a.success
                ? "text-success"
                : "text-destructive";
            return (
              <button
                key={a.result_id}
                onClick={() => onOpen?.(a.incident_id)}
                className="w-full rounded-lg border border-border/50 p-2.5 text-left transition-colors hover:border-primary/40 hover:bg-muted/30"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <Icon className={cn("h-4 w-4 shrink-0", colour)} />
                  <code className="font-mono text-xs font-semibold">{a.action.type}</code>
                  <span className="text-xs text-muted-foreground">{a.action.target}</span>
                  <Badge
                    variant="outline"
                    className={cn("ml-auto text-[10px]", effectStyle[a.decision.effect])}
                  >
                    {a.decision.effect.replace("_", " ")} · blast {a.decision.blast_radius}
                  </Badge>
                </div>
                <p className="mt-1.5 pl-6 text-xs text-muted-foreground">{a.detail}</p>
                {!a.executed && a.decision.reasons.length > 0 && (
                  <p className="mt-1 pl-6 text-[11px] italic text-muted-foreground">
                    {a.decision.reasons[0]}
                  </p>
                )}
                <div className="mt-1 pl-6 font-mono text-[10px] text-muted-foreground">
                  {new Date(a.ts).toLocaleTimeString()} · {a.actuator} · {a.duration_ms}ms
                </div>
              </button>
            );
          })}
        </div>
      </ScrollArea>
    </CardContent>
  </Card>
);
