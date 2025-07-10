import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

interface MetricCardProps {
  title: string;
  value: string;
  unit?: string;
  status: "healthy" | "warning" | "critical" | "info";
  trend?: "up" | "down" | "stable";
  change?: string;
}

export const MetricCard = ({ title, value, unit, status, trend, change }: MetricCardProps) => {
  const statusStyles = {
    healthy: "border-metric-healthy shadow-glow-success",
    warning: "border-metric-warning shadow-glow-warning",
    critical: "border-metric-critical shadow-glow-critical",
    info: "border-metric-info shadow-glow-primary"
  };

  const statusColors = {
    healthy: "text-metric-healthy",
    warning: "text-metric-warning",
    critical: "text-metric-critical",
    info: "text-metric-info"
  };

  const TrendIcon = trend === "up" ? TrendingUp : trend === "down" ? TrendingDown : Minus;

  return (
    <Card className={cn(
      "bg-card/80 backdrop-blur-sm border-2 transition-all duration-300 hover:scale-105",
      statusStyles[status]
    )}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="flex items-center justify-between">
          <div>
            <div className={cn("text-2xl font-bold", statusColors[status])}>
              {value}
              {unit && <span className="text-lg text-muted-foreground ml-1">{unit}</span>}
            </div>
            {change && (
              <div className="flex items-center gap-1 mt-1">
                <TrendIcon className={cn("h-3 w-3", statusColors[status])} />
                <span className={cn("text-xs", statusColors[status])}>{change}</span>
              </div>
            )}
          </div>
        </div>
      </CardContent>
    </Card>
  );
};