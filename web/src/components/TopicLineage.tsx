import { useEffect, useMemo, useState } from "react";
import {
  Background, BackgroundVariant, Controls, Handle, MiniMap, Position,
  ReactFlow, type Edge, type Node, type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
  Boxes, GitBranch, Layers, ShieldAlert, Sparkles, Users, Zap,
} from "lucide-react";
import { api, type LineageEdge, type LineageGraph, type LineageNode } from "@/lib/api";

/* ── node renderers ──────────────────────────────────────────────── */

const Port = ({ type, id }: { type: "source" | "target"; id?: string }) => (
  <Handle
    type={type}
    position={type === "source" ? Position.Right : Position.Left}
    id={id}
    className="!h-2 !w-2 !border-0 !bg-primary/60"
  />
);

const ProducerNode = ({ data }: NodeProps) => {
  const d = data as unknown as LineageNode;
  return (
    <div className="min-w-[150px] rounded-xl border border-sky-400/40 bg-sky-500/10 px-3 py-2 backdrop-blur-sm">
      <div className="flex items-center gap-1.5">
        <Zap className="h-3.5 w-3.5 text-sky-400" />
        <span className="text-[10px] font-semibold uppercase tracking-wider text-sky-400">
          producer
        </span>
      </div>
      <div className="mt-0.5 truncate text-sm font-medium">{d.label}</div>
      <div className="mt-1 flex items-center gap-1.5">
        <span className={cn("h-1.5 w-1.5 rounded-full",
          d.active ? "animate-pulse bg-emerald-400" : "bg-muted-foreground/40")} />
        <span className="text-[10px] text-muted-foreground">
          {d.active ? "producing" : "idle"}
        </span>
      </div>
      <Port type="source" />
    </div>
  );
};

const TopicNode = ({ data }: NodeProps) => {
  const d = data as unknown as LineageNode;
  const degraded = (d.under_replicated ?? 0) > 0;
  return (
    <div className={cn(
      "min-w-[190px] rounded-xl border-2 px-3 py-2.5 backdrop-blur-sm transition-colors",
      degraded
        ? "border-amber-400/70 bg-amber-500/10 shadow-[0_0_24px_-6px] shadow-amber-500/50"
        : "border-violet-400/50 bg-violet-500/10",
    )}>
      <div className="flex items-center gap-1.5">
        <Layers className="h-3.5 w-3.5 text-violet-300" />
        <span className="text-[10px] font-semibold uppercase tracking-wider text-violet-300">
          topic
        </span>
        {degraded && (
          <Badge className="ml-auto h-4 bg-amber-500/25 px-1.5 text-[9px] text-amber-300">
            {d.under_replicated} under-replicated
          </Badge>
        )}
      </div>
      <div className="mt-0.5 truncate font-mono text-sm font-bold">{d.label}</div>
      <div className="mt-2 grid grid-cols-3 gap-1.5 text-center">
        {[
          ["parts", d.partitions],
          ["RF", d.replication_factor],
          ["msg/s", Math.round(d.messages_per_sec ?? 0)],
        ].map(([k, v]) => (
          <div key={String(k)} className="rounded bg-background/50 py-1">
            <div className="text-[9px] uppercase text-muted-foreground">{k}</div>
            <div className="font-mono text-xs font-semibold">{v as number}</div>
          </div>
        ))}
      </div>
      <Port type="target" />
      <Port type="source" />
    </div>
  );
};

const ConsumerNode = ({ data }: NodeProps) => {
  const d = data as unknown as LineageNode;
  const lag = d.lag ?? 0;
  const pool = (d.db_pool_used ?? 0) / Math.max(d.db_pool_size ?? 1, 1);
  const saturated = pool > 0.85;
  const down = d.healthy === false;
  const laggy = lag > 500;

  const tone = down
    ? "border-rose-500/80 bg-rose-500/10 shadow-[0_0_28px_-6px] shadow-rose-500/60"
    : d.awaiting_approval
      ? "border-amber-400/80 bg-amber-500/10 shadow-[0_0_28px_-6px] shadow-amber-500/60"
      : laggy || saturated
        ? "border-orange-400/60 bg-orange-500/10"
        : "border-emerald-400/50 bg-emerald-500/10";

  // At the partition ceiling more consumers do no work — the constraint the
  // policy engine enforces, surfaced where you can see it.
  const atCeiling = (d.replicas ?? 0) >= (d.partitions ?? 0);

  return (
    <div className={cn("min-w-[210px] rounded-xl border-2 px-3 py-2.5 backdrop-blur-sm transition-all", tone)}>
      <div className="flex items-center gap-1.5">
        <Users className="h-3.5 w-3.5 text-foreground/70" />
        <span className="text-[10px] font-semibold uppercase tracking-wider text-foreground/70">
          consumer group
        </span>
        {down && (
          <Badge className="ml-auto h-4 bg-rose-500/30 px-1.5 text-[9px] text-rose-200">down</Badge>
        )}
        {d.awaiting_approval && !down && (
          <Badge className="ml-auto h-4 bg-amber-500/30 px-1.5 text-[9px] text-amber-200">
            needs approval
          </Badge>
        )}
      </div>

      <div className="mt-0.5 truncate text-sm font-bold">{d.label}</div>
      <div className="truncate font-mono text-[10px] text-muted-foreground">{d.group_id}</div>

      <div className="mt-2 grid grid-cols-2 gap-1.5">
        <div className="rounded bg-background/50 px-1.5 py-1">
          <div className="text-[9px] uppercase text-muted-foreground">lag</div>
          <div className={cn("font-mono text-xs font-semibold",
            laggy ? "text-orange-300" : "text-foreground")}>
            {lag.toLocaleString()}
          </div>
        </div>
        <div className="rounded bg-background/50 px-1.5 py-1">
          <div className="text-[9px] uppercase text-muted-foreground">replicas</div>
          <div className="font-mono text-xs font-semibold">
            {d.replicas}<span className="text-muted-foreground">/{d.partitions}</span>
            {atCeiling && <span className="ml-1 text-[9px] text-amber-400">cap</span>}
          </div>
        </div>
      </div>

      {/* resource bars */}
      <div className="mt-1.5 space-y-1">
        {[
          ["heap", (d.memory_used_pct ?? 0) / 100],
          ["pool", pool],
        ].map(([label, frac]) => (
          <div key={String(label)} className="flex items-center gap-1.5">
            <span className="w-7 text-[9px] uppercase text-muted-foreground">{label}</span>
            <div className="h-1 flex-1 overflow-hidden rounded-full bg-background/70">
              <div
                className={cn("h-full transition-all",
                  (frac as number) > 0.85 ? "bg-rose-400"
                    : (frac as number) > 0.7 ? "bg-amber-400" : "bg-emerald-400")}
                style={{ width: `${Math.min(100, (frac as number) * 100)}%` }}
              />
            </div>
            <span className="w-8 text-right font-mono text-[9px] text-muted-foreground">
              {Math.round((frac as number) * 100)}%
            </span>
          </div>
        ))}
      </div>

      {d.recent_action && (
        <div className="mt-2 flex items-start gap-1 rounded bg-primary/15 px-1.5 py-1">
          <Sparkles className="mt-0.5 h-3 w-3 shrink-0 text-primary" />
          <div className="min-w-0">
            <div className="truncate font-mono text-[9px] font-semibold text-primary">
              {d.recent_action.type}
            </div>
            <div className="text-[9px] text-muted-foreground">
              {d.recent_action.executed
                ? d.recent_action.success ? "applied by agent" : "failed"
                : "blocked by policy"}
              {d.recent_action.blast_radius !== null &&
                ` · blast ${d.recent_action.blast_radius}`}
            </div>
          </div>
        </div>
      )}

      {d.active_fault && (
        <div className="mt-1.5 flex items-center gap-1 text-[9px] text-rose-300">
          <ShieldAlert className="h-3 w-3" />
          fault injected: <span className="font-mono">{d.active_fault}</span>
        </div>
      )}

      <Port type="target" />
    </div>
  );
};

const nodeTypes = {
  producer: ProducerNode,
  topic: TopicNode,
  consumer: ConsumerNode,
};

/* ── layout ──────────────────────────────────────────────────────── */

const COLUMN_X = { producer: 0, topic: 300, consumer: 620 };

function layout(graph: LineageGraph): { nodes: Node[]; edges: Edge[] } {
  const counters: Record<string, number> = { producer: 0, topic: 0, consumer: 0 };
  // Producers stack tighter than the rows they feed, so each column gets its
  // own spacing rather than a shared grid that leaves large gaps.
  const spacing: Record<string, number> = { producer: 105, topic: 185, consumer: 185 };

  const nodes: Node[] = graph.nodes.map((n) => {
    const kind = n.kind;
    const index = counters[kind]++;
    return {
      id: n.id,
      type: kind,
      position: { x: COLUMN_X[kind], y: index * spacing[kind] },
      data: n as unknown as Record<string, unknown>,
      draggable: true,
    };
  });

  const edges: Edge[] = graph.edges.map((e: LineageEdge) => {
    const laggy = (e.lag ?? 0) > 500;
    const colour = !e.healthy ? "#fb7185" : laggy ? "#fbbf24" : "#34d399";
    return {
      id: e.id,
      source: e.source,
      target: e.target,
      animated: e.healthy && (e.rate ?? 0) > 0,
      style: {
        stroke: colour,
        strokeWidth: Math.max(1.2, Math.min(4, (e.rate ?? 0) / 60)),
        opacity: 0.85,
      },
      label: e.lag !== undefined && e.lag > 0
        ? `${Math.round(e.rate ?? 0)}/s · lag ${e.lag.toLocaleString()}`
        : `${Math.round(e.rate ?? 0)}/s`,
      labelStyle: { fill: "hsl(var(--muted-foreground))", fontSize: 9,
                    fontFamily: "ui-monospace, monospace" },
      labelBgStyle: { fill: "hsl(var(--background))", fillOpacity: 0.85 },
      labelBgPadding: [4, 2] as [number, number],
      labelBgBorderRadius: 3,
    };
  });

  return { nodes, edges };
}

/* ── component ───────────────────────────────────────────────────── */

export const TopicLineage = ({ height = 480 }: { height?: number }) => {
  const [graph, setGraph] = useState<LineageGraph | null>(null);

  useEffect(() => {
    let alive = true;
    const tick = () =>
      api.lineage().then((g) => alive && setGraph(g)).catch(() => undefined);
    tick();
    const timer = setInterval(tick, 2000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  const { nodes, edges } = useMemo(
    () => (graph ? layout(graph) : { nodes: [], edges: [] }),
    [graph],
  );

  const unhealthy = graph?.nodes.filter(
    (n) => n.kind === "consumer" && (n.healthy === false || (n.lag ?? 0) > 500),
  ).length ?? 0;

  return (
    <Card className="overflow-hidden">
      <CardHeader className="pb-2">
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          <GitBranch className="h-5 w-5 text-primary" />
          Topic lineage
          <span className="text-xs font-normal text-muted-foreground">
            producers → topics → consumer groups
          </span>
          <div className="ml-auto flex items-center gap-2">
            {unhealthy > 0 && (
              <Badge className="bg-orange-500/20 text-[10px] text-orange-300">
                {unhealthy} degraded
              </Badge>
            )}
            <Badge variant="outline" className="gap-1 text-[10px]">
              <Boxes className="h-3 w-3" />
              {graph?.nodes.length ?? 0} nodes
            </Badge>
          </div>
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <div style={{ height }} className="border-t border-border/50">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            fitViewOptions={{ padding: 0.15 }}
            minZoom={0.3}
            maxZoom={1.6}
            proOptions={{ hideAttribution: true }}
            nodesConnectable={false}
            edgesFocusable={false}
          >
            <Background variant={BackgroundVariant.Dots} gap={18} size={1}
                        className="opacity-40" />
            <Controls showInteractive={false} className="!bottom-3 !left-3" />
            <MiniMap
              pannable
              zoomable
              className="!bottom-3 !right-3 !h-20 !w-32 !bg-background/80"
              nodeColor={(n) => {
                const d = n.data as unknown as LineageNode;
                if (d.kind === "producer") return "#38bdf8";
                if (d.kind === "topic") return "#a78bfa";
                if (d.healthy === false) return "#fb7185";
                return (d.lag ?? 0) > 500 ? "#fbbf24" : "#34d399";
              }}
            />
          </ReactFlow>
        </div>
      </CardContent>
    </Card>
  );
};
