import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { LucideIcon } from "lucide-react";

interface Props {
  label: string;
  value: string;
  sub?: string;
  icon?: LucideIcon;
  tone?: "neutral" | "good" | "warn" | "bad";
}

const TONES = {
  neutral: "text-foreground",
  good: "text-emerald-400",
  warn: "text-amber-400",
  bad: "text-rose-400",
};

/** A number a non-engineer can read without a legend. */
export const StatTile = ({ label, value, sub, icon: Icon, tone = "neutral" }: Props) => (
  <Card>
    <CardContent className="p-4">
      <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
        {Icon && <Icon className="h-3.5 w-3.5" />}
        {label}
      </div>
      <div className={cn("mt-1 text-2xl font-bold tabular-nums", TONES[tone])}>
        {value}
      </div>
      {sub && <div className="mt-0.5 text-xs text-muted-foreground">{sub}</div>}
    </CardContent>
  </Card>
);
