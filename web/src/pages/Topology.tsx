import { TopicLineage } from "@/components/TopicLineage";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { ExternalLink } from "lucide-react";
import { KAFKA_UI_URL } from "@/lib/api";

/**
 * The system as a graph. Given its own page so it gets the full viewport —
 * a topology squeezed into a dashboard column is unreadable, which is what
 * happened when it shared space with the telemetry charts.
 */
const Topology = () => (
  <div className="mx-auto max-w-[1600px] space-y-4 px-4 py-5 md:px-6">
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Topology</h1>
        <p className="mt-0.5 max-w-3xl text-sm text-muted-foreground">
          Every producer, data stream and consumer group, with live throughput
          and backlog. Nodes change colour as they degrade and show what the
          agent did to them.
        </p>
      </div>
      {/* This view is the agent's interpretation. The console is the cluster
          itself — worth having one click away so the two can be compared. */}
      <Button asChild variant="outline" size="sm">
        <a href={KAFKA_UI_URL} target="_blank" rel="noreferrer">
          <ExternalLink className="mr-1.5 h-4 w-4" />
          Browse topics and messages
        </a>
      </Button>
    </div>

    <TopicLineage height={620} />

    <Card>
      <CardContent className="flex flex-wrap gap-x-6 gap-y-2 py-3 text-xs text-muted-foreground">
        {[
          ["bg-sky-400", "Producer — sends data in"],
          ["bg-violet-400", "Data stream (Kafka topic)"],
          ["bg-emerald-400", "Consumer group — healthy"],
          ["bg-amber-400", "Falling behind or awaiting approval"],
          ["bg-rose-400", "Down"],
        ].map(([dot, label]) => (
          <span key={label} className="flex items-center gap-1.5">
            <span className={`h-2 w-2 rounded-full ${dot}`} />
            {label}
          </span>
        ))}
        <span className="ml-auto">
          <code className="font-mono">cap</code> means the group already has as
          many workers as the stream can use — adding more would idle.
        </span>
      </CardContent>
    </Card>
  </div>
);

export default Topology;
