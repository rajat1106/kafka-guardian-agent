import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { ShieldAlert, Check, X } from "lucide-react";
import type { ApprovalRequest } from "@/lib/api";

const BLAST_LABEL: Record<number, string> = {
  0: "no effect",
  1: "one service, reversible",
  2: "service clients affected",
  3: "service disruption",
  4: "shared broker",
  5: "entire region",
};

interface Props {
  request: ApprovalRequest;
  onDecide: (incidentId: string, planId: string, approved: boolean) => void;
}

/**
 * The human gate. Everything needed to make the call is on the card:
 * what the agent wants to do, how far it reaches, and why policy stopped it.
 */
export const ApprovalCard = ({ request, onDecide }: Props) => {
  const action = request.actions[0];
  const decision = request.decisions[0];
  if (!action || !decision) return null;

  const severe = decision.blast_radius >= 4;

  return (
    <Card
      className={cn(
        "border-2 animate-pulse-slow",
        severe ? "border-destructive shadow-glow-critical" : "border-warning shadow-glow-warning",
      )}
    >
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <ShieldAlert className={cn("h-5 w-5", severe ? "text-destructive" : "text-warning")} />
          Approval required
          <Badge variant="outline" className="ml-auto font-mono text-xs">
            blast {decision.blast_radius}/5
          </Badge>
        </CardTitle>
      </CardHeader>

      <CardContent className="space-y-3">
        <div className="rounded-lg bg-muted/40 p-3">
          <div className="flex items-baseline gap-2">
            <code className="font-mono text-sm font-semibold">{action.type}</code>
            <span className="text-sm text-muted-foreground">→ {action.target}</span>
          </div>
          {Object.keys(action.params ?? {}).length > 0 && (
            <div className="mt-1 font-mono text-xs text-muted-foreground">
              {JSON.stringify(action.params)}
            </div>
          )}
          <p className="mt-2 text-sm text-muted-foreground">{action.rationale}</p>
        </div>

        <div className="space-y-1 text-xs">
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground">Reach:</span>
            <span>{BLAST_LABEL[decision.blast_radius] ?? "unknown"}</span>
            {!action.reversible && (
              <Badge variant="destructive" className="text-[10px]">irreversible</Badge>
            )}
          </div>
          <div className="text-muted-foreground">Policy held this because:</div>
          <ul className="ml-4 list-disc space-y-0.5">
            {decision.reasons.map((r, i) => (
              <li key={i}>{r}</li>
            ))}
          </ul>
          <div className="pt-1 font-mono text-[10px] text-muted-foreground">
            rule {decision.matched_rule} · incident {request.incident_id}
          </div>
        </div>

        <div className="flex gap-2 pt-1">
          <Button
            size="sm"
            className="flex-1"
            onClick={() => onDecide(request.incident_id, request.plan_id, true)}
          >
            <Check className="mr-1 h-4 w-4" /> Approve
          </Button>
          <Button
            size="sm"
            variant="outline"
            className="flex-1"
            onClick={() => onDecide(request.incident_id, request.plan_id, false)}
          >
            <X className="mr-1 h-4 w-4" /> Reject
          </Button>
        </div>
      </CardContent>
    </Card>
  );
};
