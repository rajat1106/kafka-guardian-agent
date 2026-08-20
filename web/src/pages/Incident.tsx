import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { ArrowLeft, Check, Copy, GitCommitHorizontal, Loader2 } from "lucide-react";
import { TimelineStep } from "@/components/IncidentTimeline";
import { api, type IncidentDetail } from "@/lib/api";
import { ago, cause as plainCause, duration } from "@/lib/plain";

/**
 * One incident, at its own URL.
 *
 * The modal this replaces could not be linked to, which meant the tool sat
 * outside the workflow it was built for: incident response is collaborative,
 * and if you cannot paste a link into the channel, people describe what they
 * saw instead of showing it. Everything here is also in the dialog — the
 * difference that matters is the address bar.
 */
const Incident = () => {
  const { id = "" } = useParams();
  const [detail, setDetail] = useState<IncidentDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    setLoading(true);
    api.incident(id)
      .then(setDetail)
      .catch(() => setDetail(null))
      .finally(() => setLoading(false));
  }, [id]);

  const outcome = detail?.outcome;
  const diagnosis = detail?.diagnosis;
  const causeKey = outcome?.root_cause ?? diagnosis?.root_cause ?? "";
  const c = plainCause(causeKey);

  const copyLink = async () => {
    try {
      await navigator.clipboard.writeText(window.location.href);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard unavailable — the URL is in the address bar regardless */
    }
  };

  return (
    <div className="mx-auto max-w-3xl space-y-4 px-4 py-5 md:px-6">
      <div className="flex flex-wrap items-center gap-2">
        <Button asChild size="sm" variant="ghost">
          <Link to="/"><ArrowLeft className="mr-1.5 h-4 w-4" />Overview</Link>
        </Button>
        <Button size="sm" variant="outline" className="ml-auto" onClick={copyLink}>
          {copied ? <Check className="mr-1.5 h-4 w-4" />
                  : <Copy className="mr-1.5 h-4 w-4" />}
          {copied ? "Copied" : "Copy link"}
        </Button>
      </div>

      {loading && (
        <Card><CardContent className="flex items-center gap-2 py-10 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading the incident…
        </CardContent></Card>
      )}

      {!loading && !detail && (
        <Card><CardContent className="py-10 text-center text-sm text-muted-foreground">
          No incident found with id <code className="font-mono">{id}</code>.
          It may have rolled out of the dashboard's retained history.
        </CardContent></Card>
      )}

      {!loading && detail && (
        <>
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="flex flex-wrap items-center gap-2">
                {c.title}
                {detail.service && (
                  <span className="text-sm font-normal text-muted-foreground">
                    on {detail.service}
                  </span>
                )}
                {outcome && (
                  <Badge className={cn("ml-auto text-[10px]", outcome.resolved
                    ? "bg-emerald-500/20 text-emerald-300"
                    : "bg-amber-500/20 text-amber-300")}>
                    {outcome.resolved ? "resolved" : "escalated"}
                  </Badge>
                )}
              </CardTitle>
              <p className="text-sm text-muted-foreground">{c.meaning}</p>
            </CardHeader>
            <CardContent>
              <dl className="grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
                {[
                  ["When", outcome ? ago(outcome.ts) : "in progress"],
                  ["Time to resolve", outcome ? duration(outcome.mttr_seconds) : "—"],
                  ["Decided by", diagnosis?.source === "llm"
                    ? (diagnosis.model ?? "model") : "rules engine"],
                  ["Human approved", outcome?.human_approved ? "yes" : "no"],
                ].map(([k, v]) => (
                  <div key={k} className="rounded-lg bg-muted/40 px-2.5 py-2">
                    <dt className="text-[10px] uppercase text-muted-foreground">{k}</dt>
                    <dd className="mt-0.5 font-medium">{v}</dd>
                  </div>
                ))}
              </dl>

              {diagnosis?.correlated_changes?.length ? (
                <div className="mt-3 rounded-lg bg-muted/30 px-3 py-2">
                  <div className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    <GitCommitHorizontal className="h-3 w-3" /> What changed first
                  </div>
                  {diagnosis.correlated_changes.map((ch, i) => (
                    <p key={i} className="mt-0.5 text-xs text-muted-foreground">{ch}</p>
                  ))}
                </div>
              ) : null}

              {!outcome?.resolved && (
                <p className="mt-3 text-xs text-amber-400/90">
                  Risk if ignored: {c.risk}
                </p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base">What happened</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-4 border-l border-border/50 pl-3">
                {detail.timeline.length === 0 && (
                  <p className="text-sm text-muted-foreground">
                    No recorded steps for this incident.
                  </p>
                )}
                {detail.timeline.map((entry, i) => (
                  <TimelineStep key={`${entry.kind}-${i}`} entry={entry} />
                ))}
              </div>
              <p className="mt-4 border-t border-border/50 pt-2 font-mono text-[10px] text-muted-foreground">
                incident {detail.incident_id}
              </p>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
};

export default Incident;
