import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { AlertTriangle, CheckCircle, Clock, Bot } from "lucide-react";

interface AlertCardProps {
  id: string;
  title: string;
  description: string;
  severity: "critical" | "warning" | "info";
  status: "active" | "investigating" | "resolved";
  timestamp: Date;
  source: string;
  aiValidation?: "validating" | "true" | "false";
  onInvestigate?: (id: string) => void;
}

export const AlertCard = ({ 
  id, 
  title, 
  description, 
  severity, 
  status, 
  timestamp, 
  source,
  aiValidation,
  onInvestigate 
}: AlertCardProps) => {
  const severityStyles = {
    critical: "border-critical shadow-glow-critical",
    warning: "border-warning shadow-glow-warning",
    info: "border-info shadow-glow-primary"
  };

  const severityColors = {
    critical: "text-critical",
    warning: "text-warning",
    info: "text-info"
  };

  const StatusIcon = status === "resolved" ? CheckCircle : AlertTriangle;

  const getValidationBadge = () => {
    if (!aiValidation) return null;
    
    const variants = {
      validating: { color: "bg-ai-primary text-white", icon: Bot, text: "AI Validating...", animate: true },
      true: { color: "bg-critical text-white", icon: AlertTriangle, text: "Confirmed Alert", animate: false },
      false: { color: "bg-success text-white", icon: CheckCircle, text: "False Positive", animate: false }
    };

    const variant = variants[aiValidation];
    const Icon = variant.icon;

    return (
      <Badge className={cn(
        variant.color,
        variant.animate && "animate-pulse"
      )}>
        <Icon className="h-3 w-3 mr-1" />
        {variant.text}
      </Badge>
    );
  };

  return (
    <Card className={cn(
      "bg-card/90 backdrop-blur-sm border-2 transition-all duration-300 hover:scale-102",
      severityStyles[severity],
      status === "active" && "animate-pulse-glow"
    )}>
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-2">
            <StatusIcon className={cn("h-4 w-4", severityColors[severity])} />
            <CardTitle className="text-sm font-medium">
              {title}
            </CardTitle>
          </div>
          <div className="flex flex-col gap-1 items-end">
            <Badge variant={status === "resolved" ? "secondary" : "destructive"}>
              {status}
            </Badge>
            {getValidationBadge()}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground mb-3">{description}</p>
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <div className="flex items-center gap-2">
            <Clock className="h-3 w-3" />
            <span>{timestamp.toLocaleTimeString()}</span>
            <span>•</span>
            <span>{source}</span>
          </div>
          {status === "active" && onInvestigate && (
            <Button 
              size="sm" 
              variant="outline"
              onClick={() => onInvestigate(id)}
              className="h-6 px-2 text-xs"
            >
              Investigate
            </Button>
          )}
        </div>
      </CardContent>
    </Card>
  );
};