import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { Cloud, Server, Wifi, WifiOff, Coins } from "lucide-react";
import type { ClusterInfo } from "@/lib/api";

interface Props {
  cluster: ClusterInfo | null;
  connected: boolean;
}

/**
 * Which cluster the agent is attached to, what it may do there, and how much
 * of the token budget is left. The capability list is the honest part: on a
 * managed cluster several remedies simply do not exist.
 */
export const ClusterBanner = ({ cluster, connected }: Props) => {
  const managed = cluster?.provider === "confluent";
  const Icon = managed ? Cloud : Server;
  const budget = cluster?.guardian?.budget;
  const dayPct = budget ? Math.min(100, (budget.day_used / budget.day_limit) * 100) : 0;

  const blocked = Object.entries(cluster?.capabilities ?? {})
    .filter(([k, v]) => k.startsWith("can_") && v === false)
    .map(([k]) => k.replace("can_", "").replace(/_/g, " "));

  return (
    <Card className="border-border/60">
      <CardContent className="flex flex-wrap items-center gap-x-6 gap-y-2 py-3">
        <div className="flex items-center gap-2">
          <Icon className="h-4 w-4 text-primary" />
          <span className="text-sm font-medium">{cluster?.description ?? "connecting…"}</span>
          {managed && <Badge variant="outline" className="text-[10px]">managed</Badge>}
        </div>

        <div className="flex items-center gap-1.5 text-xs">
          {connected ? (
            <><Wifi className="h-3.5 w-3.5 text-success" /><span className="text-success">live</span></>
          ) : (
            <><WifiOff className="h-3.5 w-3.5 text-destructive" /><span className="text-destructive">reconnecting…</span></>
          )}
        </div>

        {blocked.length > 0 && (
          <div className="text-xs text-muted-foreground">
            unavailable here: <span className="text-warning">{blocked.join(", ")}</span>
          </div>
        )}

        {cluster?.guardian?.planner && (
          <div className="text-xs text-muted-foreground">
            planner: <span className="font-mono">{cluster.guardian.planner}</span>
          </div>
        )}

        {budget && (
          <div className="ml-auto flex items-center gap-2 text-xs">
            <Coins className="h-3.5 w-3.5 text-muted-foreground" />
            <div className="h-1.5 w-24 overflow-hidden rounded-full bg-muted">
              <div
                className={cn(
                  "h-full transition-all",
                  dayPct > 80 ? "bg-destructive" : dayPct > 50 ? "bg-warning" : "bg-success",
                )}
                style={{ width: `${dayPct}%` }}
              />
            </div>
            <span className="font-mono text-muted-foreground">
              {budget.day_used.toLocaleString()}/{budget.day_limit.toLocaleString()} tok ·
              ${budget.day_cost_usd.toFixed(3)}
            </span>
          </div>
        )}
      </CardContent>
    </Card>
  );
};
