import { Link } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ArrowLeft, Cloud, Server, ShieldCheck, ShieldX } from "lucide-react";
import { cn } from "@/lib/utils";
import { useGuardian } from "@/hooks/useGuardian";

/**
 * Read-only cluster view.
 *
 * The previous version of this page was a form for typing broker addresses
 * and SASL credentials into the browser. That is removed deliberately: the
 * cluster is configured server-side through environment variables, and a
 * Confluent Cloud API secret has no business being held in front-end state
 * or sent to a browser. What remains is the part that is actually useful —
 * seeing which cluster the agent is attached to and what it may do there.
 */
const KafkaConfig = () => {
  const { cluster, connected } = useGuardian();
  const managed = cluster?.provider === "confluent";
  const Icon = managed ? Cloud : Server;

  const capabilities = Object.entries(cluster?.capabilities ?? {}).filter(([k]) =>
    k.startsWith("can_"),
  );

  return (
    <div className="min-h-screen bg-background p-4 md:p-6">
      <div className="mx-auto max-w-4xl space-y-4">
        <div className="flex items-center gap-3">
          <Button asChild size="sm" variant="ghost">
            <Link to="/"><ArrowLeft className="mr-1.5 h-4 w-4" />Dashboard</Link>
          </Button>
          <h1 className="text-xl font-bold">Cluster</h1>
          <Badge variant={connected ? "outline" : "destructive"} className="ml-auto">
            {connected ? "live" : "disconnected"}
          </Badge>
        </div>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="flex items-center gap-2 text-base">
              <Icon className="h-5 w-5 text-primary" />
              {cluster?.description ?? "connecting…"}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <dl className="grid grid-cols-1 gap-3 text-sm sm:grid-cols-2">
              {[
                ["Provider", cluster?.provider ?? "—"],
                ["Bootstrap servers", cluster?.bootstrap ?? "—"],
                ["Security protocol", cluster?.security_protocol ?? "—"],
                ["Topic prefix", cluster?.topic_prefix || "(none)"],
                ["Replication factor", String(cluster?.replication_factor ?? "—")],
                ["Planner", cluster?.guardian?.planner ?? "—"],
              ].map(([k, v]) => (
                <div key={k} className="rounded-lg bg-muted/40 px-3 py-2">
                  <dt className="text-xs text-muted-foreground">{k}</dt>
                  <dd className="font-mono text-sm break-all">{v}</dd>
                </div>
              ))}
            </dl>
            <p className="mt-4 text-xs text-muted-foreground">
              Configured server-side via <code className="font-mono">KAFKA_*</code> environment
              variables. Credentials are never sent to the browser.
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-base">What the agent may do here</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid gap-2 sm:grid-cols-2">
              {capabilities.map(([name, allowed]) => (
                <div
                  key={name}
                  className={cn(
                    "flex items-center gap-2 rounded-lg border px-3 py-2 text-sm",
                    allowed
                      ? "border-success/30 bg-success/5"
                      : "border-warning/30 bg-warning/5",
                  )}
                >
                  {allowed ? (
                    <ShieldCheck className="h-4 w-4 shrink-0 text-success" />
                  ) : (
                    <ShieldX className="h-4 w-4 shrink-0 text-warning" />
                  )}
                  <span className="font-mono text-xs">{name.replace("can_", "")}</span>
                </div>
              ))}
            </div>
            {typeof cluster?.capabilities?.notes === "string" && (
              <p className="mt-3 text-xs text-muted-foreground">
                {cluster.capabilities.notes}
              </p>
            )}
          </CardContent>
        </Card>

        {cluster?.guardian?.memory && (
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Incident memory</CardTitle>
            </CardHeader>
            <CardContent>
              <dl className="grid grid-cols-2 gap-3 text-sm sm:grid-cols-3">
                {Object.entries(cluster.guardian.memory).map(([k, v]) => (
                  <div key={k} className="rounded-lg bg-muted/40 px-3 py-2">
                    <dt className="text-xs text-muted-foreground">{k.replace(/_/g, " ")}</dt>
                    <dd className="font-mono text-sm">{String(v)}</dd>
                  </div>
                ))}
              </dl>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
};

export default KafkaConfig;
