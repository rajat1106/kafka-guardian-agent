import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { Brain, Cpu, Coins } from "lucide-react";
import type { Diagnosis } from "@/lib/api";

interface Props {
  diagnosis: Diagnosis | null;
  planner: string;
}

/**
 * The agent's reasoning, shown verbatim.
 *
 * The distinction that matters here is `source`: whether a conclusion came
 * from the LLM or the deterministic planner changes how much weight it
 * deserves, so it is labelled rather than blurred into one "AI" badge.
 */
export const AgentReasoning = ({ diagnosis, planner }: Props) => {
  if (!diagnosis) {
    return (
      <Card className="border-primary/20">
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Brain className="h-5 w-5 text-muted-foreground" />
            Agent idle
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Watching for anomalies. No incident is open.
          </p>
          <p className="mt-2 font-mono text-xs text-muted-foreground">planner: {planner}</p>
        </CardContent>
      </Card>
    );
  }

  const isLLM = diagnosis.source === "llm";
  const confident = diagnosis.confidence >= 0.7;

  return (
    <Card className="border-primary/30 shadow-glow-ai">
      <CardHeader className="pb-3">
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          <Brain className="h-5 w-5 text-primary" />
          <code className="font-mono text-sm">{diagnosis.root_cause}</code>
          <Badge
            className={cn(
              "ml-auto",
              confident ? "bg-success/20 text-success" : "bg-warning/20 text-warning",
            )}
          >
            {(diagnosis.confidence * 100).toFixed(0)}% confidence
          </Badge>
        </CardTitle>
      </CardHeader>

      <CardContent className="space-y-3">
        <p className="text-sm leading-relaxed">{diagnosis.reasoning}</p>

        {diagnosis.evidence.length > 0 && (
          <div>
            <div className="mb-1 text-xs font-medium text-muted-foreground">Evidence</div>
            <ul className="space-y-1">
              {diagnosis.evidence.slice(0, 4).map((e, i) => (
                <li key={i} className="font-mono text-xs text-muted-foreground">• {e}</li>
              ))}
            </ul>
          </div>
        )}

        {diagnosis.similar_incidents.length > 0 && (
          <div className="text-xs text-muted-foreground">
            Recalled {diagnosis.similar_incidents.length} similar past incident
            {diagnosis.similar_incidents.length > 1 ? "s" : ""}
          </div>
        )}

        <div className="flex flex-wrap items-center gap-3 border-t border-border/50 pt-2 text-xs text-muted-foreground">
          <span className="flex items-center gap-1">
            <Cpu className="h-3 w-3" />
            {isLLM ? diagnosis.model ?? "llm" : "deterministic planner"}
          </span>
          {diagnosis.tokens_used > 0 && (
            <span className="flex items-center gap-1">
              <Coins className="h-3 w-3" />
              {diagnosis.tokens_used.toLocaleString()} tokens · ${diagnosis.cost_usd.toFixed(4)}
            </span>
          )}
          {diagnosis.latency_ms > 0 && <span>{diagnosis.latency_ms} ms</span>}
        </div>
      </CardContent>
    </Card>
  );
};
