import { useState, useEffect } from "react";
import { MetricCard } from "@/components/MetricCard";
import { AlertCard } from "@/components/AlertCard";
import { AIAgent } from "@/components/AIAgent";
import { TopicLineage } from "@/components/TopicLineage";
import { showNotification } from "@/components/NotificationToast";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Server, Activity, Zap, AlertTriangle, ExternalLink } from "lucide-react";

// Mock data types
interface KafkaMetric {
  id: string;
  name: string;
  value: string;
  unit?: string;
  status: "healthy" | "warning" | "critical" | "info";
  trend?: "up" | "down" | "stable";
  change?: string;
}

interface Alert {
  id: string;
  title: string;
  description: string;
  severity: "critical" | "warning" | "info";
  status: "active" | "investigating" | "resolved";
  timestamp: Date;
  source: string;
  aiValidation?: "validating" | "true" | "false";
}

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

const Index = () => {
  const [metrics, setMetrics] = useState<KafkaMetric[]>([
    {
      id: "1",
      name: "Messages/sec",
      value: "1,247",
      unit: "msg/s",
      status: "healthy",
      trend: "up",
      change: "+5.2%"
    },
    {
      id: "2",
      name: "Consumer Lag",
      value: "234",
      unit: "msgs",
      status: "warning",
      trend: "up",
      change: "+12%"
    },
    {
      id: "3",
      name: "Broker CPU",
      value: "78",
      unit: "%",
      status: "warning",
      trend: "up",
      change: "+8%"
    },
    {
      id: "4",
      name: "Disk Usage",
      value: "45",
      unit: "%",
      status: "healthy",
      trend: "stable",
      change: "0%"
    },
    {
      id: "5",
      name: "Network I/O",
      value: "892",
      unit: "MB/s",
      status: "info",
      trend: "up",
      change: "+3%"
    },
    {
      id: "6",
      name: "Active Connections",
      value: "156",
      status: "healthy",
      trend: "stable",
      change: "0%"
    }
  ]);

  const [alerts, setAlerts] = useState<Alert[]>([
    {
      id: "alert-1",
      title: "High Consumer Lag Detected",
      description: "Consumer group 'analytics-group' showing lag of 234 messages on topic 'user-events'",
      severity: "warning",
      status: "active",
      timestamp: new Date(Date.now() - 5 * 60 * 1000),
      source: "SignalFx",
      aiValidation: "validating"
    },
    {
      id: "alert-2",
      title: "Broker CPU Usage Critical",
      description: "Broker kafka-01 CPU usage at 78% for over 5 minutes",
      severity: "critical",
      status: "active",
      timestamp: new Date(Date.now() - 3 * 60 * 1000),
      source: "SignalFx",
      aiValidation: "true"
    }
  ]);

  const [aiAgent, setAiAgent] = useState<{
    status: "idle" | "analyzing" | "deciding" | "executing";
    currentDecision?: AIDecision;
    recentDecisions: AIDecision[];
  }>({
    status: "analyzing",
    currentDecision: {
      id: "decision-1",
      alertId: "alert-2",
      decision: "Scale up broker instances",
      confidence: 89,
      reasoning: "CPU usage pattern indicates sustained load. Historical data shows this resolves 87% of similar incidents.",
      actions: [
        "Increase broker instance count from 3 to 5",
        "Rebalance partition leaders",
        "Monitor for 10 minutes"
      ],
      status: "decided",
      timestamp: new Date()
    },
    recentDecisions: [
      {
        id: "decision-2",
        alertId: "alert-3",
        decision: "Restart consumer group",
        confidence: 92,
        reasoning: "Consumer lag due to stuck consumer threads",
        actions: ["Restart consumer group", "Monitor recovery"],
        status: "completed",
        timestamp: new Date(Date.now() - 10 * 60 * 1000)
      }
    ]
  });

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      // Update metrics
      setMetrics(prev => prev.map(metric => ({
        ...metric,
        value: metric.name === "Messages/sec" 
          ? (Math.floor(Math.random() * 200) + 1100).toLocaleString()
          : metric.value
      })));

      // Simulate AI validation completion
      if (Math.random() > 0.7) {
        setAlerts(prev => prev.map(alert => {
          if (alert.aiValidation === "validating") {
            const isTrue = Math.random() > 0.3;
            if (isTrue) {
              showNotification("validation", "Alert Confirmed", `AI confirmed: ${alert.title}`);
            } else {
              showNotification("validation", "False Positive Detected", `AI dismissed: ${alert.title}`);
            }
            return { ...alert, aiValidation: isTrue ? "true" : "false" };
          }
          return alert;
        }));
      }
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const handleInvestigateAlert = (alertId: string) => {
    const alert = alerts.find(a => a.id === alertId);
    if (alert) {
      setAlerts(prev => prev.map(a => 
        a.id === alertId ? { ...a, status: "investigating" as const } : a
      ));
      
      setAiAgent(prev => ({
        ...prev,
        status: "analyzing",
        currentDecision: {
          id: `decision-${Date.now()}`,
          alertId,
          decision: "Analyzing root cause...",
          confidence: 0,
          reasoning: "Gathering additional metrics and historical patterns",
          actions: [],
          status: "analyzing",
          timestamp: new Date()
        }
      }));

      showNotification("info", "Investigation Started", `AI agent is analyzing: ${alert.title}`);
    }
  };

  const handleControlCenter = () => {
    // In a real app, this would open Kafka Control Center
    window.open('http://localhost:9021', '_blank');
  };

  return (
    <div className="min-h-screen bg-background p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold bg-gradient-to-r from-primary to-ai-primary bg-clip-text text-transparent">
              Kafka Intelligence Platform
            </h1>
            <p className="text-muted-foreground">
              AI-powered monitoring and automated healing for Apache Kafka
            </p>
          </div>
          <div className="flex items-center gap-4">
            <Badge className="bg-success/20 text-success border-success">
              <Activity className="h-3 w-3 mr-1" />
              System Healthy
            </Badge>
            <Badge className="bg-ai-primary text-white">
              <Zap className="h-3 w-3 mr-1" />
              AI Agent Active
            </Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={handleControlCenter}
              className="flex items-center gap-2"
            >
              <ExternalLink className="h-4 w-4" />
              Kafka Control Center
            </Button>
          </div>
        </div>

        {/* Main Content */}
        <Tabs defaultValue="overview" className="space-y-6">
          <TabsList className="grid w-full grid-cols-5">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="metrics">Metrics</TabsTrigger>
            <TabsTrigger value="alerts">Alerts</TabsTrigger>
            <TabsTrigger value="topic-lineage">Topic Lineage</TabsTrigger>
            <TabsTrigger value="ai-agent">AI Agent</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="space-y-6">
            {/* Key Metrics Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {metrics.slice(0, 6).map((metric) => (
                <MetricCard key={metric.id} title={metric.name} {...metric} />
              ))}
            </div>

            {/* Dashboard Layout */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Alerts Column */}
              <div className="lg:col-span-2 space-y-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                      <AlertTriangle className="h-5 w-5 text-warning" />
                      Active Alerts
                      <Badge variant="destructive">{alerts.filter(a => a.status === "active").length}</Badge>
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    {alerts.map((alert) => (
                      <AlertCard 
                        key={alert.id} 
                        {...alert} 
                        onInvestigate={handleInvestigateAlert}
                      />
                    ))}
                  </CardContent>
                </Card>
              </div>

              {/* AI Agent Column */}
              <div>
                <AIAgent {...aiAgent} />
              </div>
            </div>
          </TabsContent>

          <TabsContent value="metrics" className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {metrics.map((metric) => (
                <MetricCard key={metric.id} title={metric.name} {...metric} />
              ))}
            </div>
          </TabsContent>

          <TabsContent value="alerts" className="space-y-4">
            {alerts.map((alert) => (
              <AlertCard 
                key={alert.id} 
                {...alert} 
                onInvestigate={handleInvestigateAlert}
              />
            ))}
          </TabsContent>

          <TabsContent value="topic-lineage">
            <TopicLineage />
          </TabsContent>

          <TabsContent value="ai-agent">
            <AIAgent {...aiAgent} />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
};

export default Index;
