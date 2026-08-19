import { Link, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Activity, Bot, Network, Plug, ShieldQuestion } from "lucide-react";

const TABS = [
  { to: "/", label: "Overview", icon: Activity,
    hint: "What the agent has done" },
  { to: "/topology", label: "Topology", icon: Network,
    hint: "How the system is wired" },
  { to: "/operations", label: "Operations", icon: ShieldQuestion,
    hint: "Detailed telemetry and audit trail" },
  { to: "/connections", label: "Connections", icon: Plug,
    hint: "Plug in your cluster, model and alerts" },
];

interface Props {
  connected: boolean;
  pendingApprovals: number;
  clusterLabel?: string;
}

export const AppNav = ({ connected, pendingApprovals, clusterLabel }: Props) => {
  const { pathname } = useLocation();
  return (
    <header className="sticky top-0 z-30 border-b border-border/60 bg-background/85 backdrop-blur">
      <div className="mx-auto flex max-w-[1600px] flex-wrap items-center gap-x-6 gap-y-2 px-4 py-2.5 md:px-6">
        <Link to="/" className="flex items-center gap-2">
          <div className="rounded-lg bg-primary/15 p-1.5">
            <Bot className="h-5 w-5 text-primary" />
          </div>
          <div className="leading-tight">
            <div className="text-sm font-bold tracking-tight">Kafka Guardian</div>
            <div className="text-[10px] text-muted-foreground">
              {clusterLabel ?? "connecting…"}
            </div>
          </div>
        </Link>

        <nav className="flex flex-1 flex-wrap items-center gap-1">
          {TABS.map(({ to, label, icon: Icon }) => {
            const active = to === "/" ? pathname === "/" : pathname.startsWith(to);
            return (
              <Link
                key={to}
                to={to}
                className={cn(
                  "flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm transition-colors",
                  active
                    ? "bg-primary/15 font-medium text-primary"
                    : "text-muted-foreground hover:bg-muted/60 hover:text-foreground",
                )}
              >
                <Icon className="h-4 w-4" />
                {label}
                {to === "/connections" && pendingApprovals === -1 && null}
              </Link>
            );
          })}
        </nav>

        <div className="flex items-center gap-3">
          {pendingApprovals > 0 && (
            <Badge className="animate-pulse bg-amber-500/20 text-amber-300">
              {pendingApprovals} need{pendingApprovals === 1 ? "s" : ""} your approval
            </Badge>
          )}
          <span className="flex items-center gap-1.5 text-xs">
            <span className={cn("h-2 w-2 rounded-full",
              connected ? "animate-pulse bg-emerald-400" : "bg-rose-400")} />
            <span className={connected ? "text-emerald-400" : "text-rose-400"}>
              {connected ? "live" : "reconnecting"}
            </span>
          </span>
        </div>
      </div>
    </header>
  );
};
