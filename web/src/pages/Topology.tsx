import { TopicLineage } from "@/components/TopicLineage";
import { Card, CardContent } from "@/components/ui/card";

/**
 * The system as a graph. Given its own page so it gets the full viewport —
 * a topology squeezed into a dashboard column is unreadable, which is what
 * happened when it shared space with the telemetry charts.
 */
const Topology = () => (
  <div className="mx-auto max-w-[1600px] space-y-4 px-4 py-5 md:px-6">
    <div>
      <h1 className="text-2xl font-bold tracking-tight">Topology</h1>
      <p className="mt-0.5 text-sm text-muted-foreground">
        Every producer, data stream and consumer group, with live throughput and
        backlog. Nodes change colour as they degrade and show what the agent did
        to them.
      </p>
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
