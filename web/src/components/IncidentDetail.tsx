import { useEffect, useState } from "react";
import {
  Dialog, DialogContent, DialogHeader, DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import { ExternalLink, Loader2 } from "lucide-react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { api, type IncidentDetail as Detail } from "@/lib/api";
import { TimelineStep } from "@/components/IncidentTimeline";
import {
  ago, action as plainAction, cause as plainCause, confidenceWord,
  duration, metric as plainMetric,
} from "@/lib/plain";

interface Props {
  incidentId: string | null;
  onClose: () => void;
}

export const IncidentDetailDialog = ({ incidentId, onClose }: Props) => {
  const [detail, setDetail] = useState<Detail | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!incidentId) { setDetail(null); return; }
    setLoading(true);
    api.incident(incidentId)
      .then(setDetail)
      .catch(() => setDetail(null))
      .finally(() => setLoading(false));
  }, [incidentId]);

  const outcome = detail?.outcome;
  const title = outcome
    ? plainCause(outcome.root_cause).title
    : detail?.diagnosis
      ? plainCause(detail.diagnosis.root_cause).title
      : "Incident";

  return (
    <Dialog open={Boolean(incidentId)} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-h-[85vh] max-w-2xl overflow-hidden">
        <DialogHeader>
          <DialogTitle className="flex flex-wrap items-center gap-2 text-base">
            {title}
            {detail?.service && (
              <span className="text-sm font-normal text-muted-foreground">
                on {detail.service}
              </span>
            )}
            {outcome && (
              <span className="ml-auto text-xs font-normal text-muted-foreground">
                {ago(outcome.ts)}
              </span>
            )}
          </DialogTitle>
          {outcome && (
            <p className="text-xs text-muted-foreground">
              {plainCause(outcome.root_cause).meaning}
            </p>
          )}
        </DialogHeader>

        {loading && (
          <div className="flex items-center gap-2 py-8 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading the timeline…
          </div>
        )}

        {!loading && detail && (
          <ScrollArea className="max-h-[60vh] pr-3">
            <div className="space-y-4 border-l border-border/50 pl-3">
              {detail.timeline.length === 0 && (
                <p className="text-sm text-muted-foreground">
                  No recorded steps for this incident. It may have rolled out of
                  the dashboard's retained history.
                </p>
              )}
              {detail.timeline.map((entry, i) => (
                <TimelineStep key={`${entry.kind}-${i}`} entry={entry} />
              ))}
            </div>
            <div className="mt-4 flex items-center gap-2 border-t border-border/50 pt-2">
              <span className="font-mono text-[10px] text-muted-foreground">
                incident {detail.incident_id}
              </span>
              <Button asChild size="sm" variant="ghost" className="ml-auto h-6 text-[11px]">
                <Link to={`/incidents/${detail.incident_id}`} onClick={onClose}>
                  Open full page <ExternalLink className="ml-1 h-3 w-3" />
                </Link>
              </Button>
            </div>
          </ScrollArea>
        )}
      </DialogContent>
    </Dialog>
  );
};
