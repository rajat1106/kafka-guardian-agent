import { toast } from "@/hooks/use-toast";
import { AlertTriangle, CheckCircle, Info, Bot } from "lucide-react";

export const showNotification = (
  type: "alert" | "action" | "validation" | "info",
  title: string,
  description: string
) => {
  const icons = {
    alert: AlertTriangle,
    action: CheckCircle,
    validation: Bot,
    info: Info
  };

  const variants = {
    alert: "destructive",
    action: "default",
    validation: "default",
    info: "default"
  } as const;

  const Icon = icons[type];

  toast({
    title,
    description: (
      <div className="flex items-center gap-2">
        <Icon className="h-4 w-4" />
        {description}
      </div>
    ),
    variant: variants[type],
  });
};