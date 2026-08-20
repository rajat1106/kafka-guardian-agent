import { Link, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import { Activity, Bot, LogOut, Network, Plug, ShieldQuestion } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { Principal } from "@/lib/auth";

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
  principal?: Principal | null;
  onSignOut?: () => void;
}

export const AppNav = ({
  connected, pendingApprovals, clusterLabel, principal, onSignOut,
}: Props) => {
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

          {principal && (
            <div className="flex items-center gap-2 border-l border-border/60 pl-3">
              <div className="text-right leading-tight">
                <div className="text-xs font-medium">{principal.display_name}</div>
                <div className="text-[10px] text-muted-foreground">
                  {principal.roles.join(", ")} · approves to blast{" "}
                  {principal.max_blast}
                </div>
              </div>
              <Button size="sm" variant="ghost" className="h-7 px-2"
                      onClick={onSignOut} title="Sign out">
                <LogOut className="h-3.5 w-3.5" />
              </Button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
};
