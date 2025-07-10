import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { Bot, Brain, Zap, CheckCircle, AlertCircle, Clock } from "lucide-react";

interface AIDecision {
  id: string;
  alertId: string;
  decision: string;
  confidence: number;
  reasoning: string;
  actions: string[];
  status: "analyzing" | "decided" | "executing" | "completed";
  timestamp: Date;
}

interface AIAgentProps {
  status: "idle" | "analyzing" | "deciding" | "executing";
  currentDecision?: AIDecision;
  recentDecisions: AIDecision[];
}

export const AIAgent = ({ status, currentDecision, recentDecisions }: AIAgentProps) => {
  const statusInfo = {
    idle: { icon: Bot, text: "Idle", color: "text-muted-foreground", bgColor: "bg-muted" },
    analyzing: { icon: Brain, text: "Analyzing Metrics", color: "text-ai-primary", bgColor: "bg-ai-primary/20" },
    deciding: { icon: Brain, text: "Making Decision", color: "text-ai-secondary", bgColor: "bg-ai-secondary/20" },
    executing: { icon: Zap, text: "Executing Actions", color: "text-warning", bgColor: "bg-warning/20" }
  };

  const currentStatus = statusInfo[status];
  const CurrentIcon = currentStatus.icon;

  return (
    <Card className="bg-card/90 backdrop-blur-sm border-ai-primary/30 shadow-glow-ai">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <div className={cn(
            "p-2 rounded-full",
            currentStatus.bgColor,
            status !== "idle" && "animate-ai-thinking"
          )}>
            <CurrentIcon className={cn("h-5 w-5", currentStatus.color)} />
          </div>
          AI Healing Agent
          <Badge 
            className={cn(
              "ml-auto",
              currentStatus.bgColor,
              currentStatus.color
            )}
          >
            {currentStatus.text}
          </Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {currentDecision && (
          <div className="p-3 rounded-lg bg-ai-primary/10 border border-ai-primary/20">
            <div className="flex items-center gap-2 mb-2">
              <Badge className="bg-ai-primary text-white">
                {currentDecision.confidence}% Confidence
              </Badge>
              <span className="text-sm text-muted-foreground">
                {currentDecision.timestamp.toLocaleTimeString()}
              </span>
            </div>
            <h4 className="font-medium mb-1">{currentDecision.decision}</h4>
            <p className="text-sm text-muted-foreground mb-2">{currentDecision.reasoning}</p>
            {currentDecision.actions.length > 0 && (
              <div className="space-y-1">
                <span className="text-xs font-medium text-muted-foreground">Planned Actions:</span>
                {currentDecision.actions.map((action, index) => (
                  <div key={index} className="flex items-center gap-2 text-xs">
                    {currentDecision.status === "completed" ? (
                      <CheckCircle className="h-3 w-3 text-success" />
                    ) : currentDecision.status === "executing" ? (
                      <Clock className="h-3 w-3 text-warning animate-spin" />
                    ) : (
                      <AlertCircle className="h-3 w-3 text-muted-foreground" />
                    )}
                    <span>{action}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        <div className="space-y-2">
          <h4 className="text-sm font-medium text-muted-foreground">Recent Decisions</h4>
          <div className="space-y-2 max-h-40 overflow-y-auto">
            {recentDecisions.slice(0, 3).map((decision) => (
              <div key={decision.id} className="p-2 rounded bg-muted/30 text-xs">
                <div className="flex items-center justify-between mb-1">
                  <span className="font-medium">{decision.decision}</span>
                  <Badge 
                    variant={decision.status === "completed" ? "secondary" : "outline"}
                    className="text-xs"
                  >
                    {decision.status}
                  </Badge>
                </div>
                <span className="text-muted-foreground">
                  {decision.timestamp.toLocaleTimeString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      </CardContent>
    </Card>
  );
};