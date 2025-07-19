import { useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Database } from 'lucide-react';
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { KafkaConnection } from '@/components/KafkaConnection';
import { useKafka } from '@/hooks/useKafka';

const KafkaConfig = () => {
  const kafka = useKafka();

  return (
    <div className="min-h-screen bg-gradient-to-br from-background via-background to-background/80">
      <div className="container mx-auto px-4 py-8">
        {/* Header */}
        <div className="flex items-center gap-4 mb-8">
          <Link to="/">
            <Button variant="outline" size="sm" className="flex items-center gap-2">
              <ArrowLeft className="h-4 w-4" />
              Back to Dashboard
            </Button>
          </Link>
          <div className="flex items-center gap-3">
            <Database className="h-8 w-8 text-primary" />
            <div>
              <h1 className="text-3xl font-bold text-foreground">Kafka Configuration</h1>
              <p className="text-muted-foreground">Connect and configure your Kafka cluster</p>
            </div>
          </div>
        </div>

        {/* Connection Status Banner */}
        {kafka.connected && (
          <Card className="mb-6 border-success/20 bg-success/5">
            <CardContent className="p-4">
              <div className="flex items-center gap-3">
                <Badge className="bg-success/20 text-success">Connected</Badge>
                <span className="text-sm text-foreground">
                  Successfully connected to Kafka cluster with {kafka.lineageData?.topics.length || 0} topics
                </span>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Main Configuration */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Connection Configuration */}
          <div className="lg:col-span-2">
            <KafkaConnection kafka={kafka} />
          </div>

          {/* Info Panel */}
          <div className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="text-lg">Connection Info</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <h4 className="font-medium text-sm">Supported Protocols</h4>
                  <ul className="text-sm text-muted-foreground space-y-1">
                    <li>• PLAINTEXT (localhost)</li>
                    <li>• SSL/TLS</li>
                    <li>• SASL_SSL</li>
                    <li>• SASL_PLAINTEXT</li>
                  </ul>
                </div>
                
                <div className="space-y-2">
                  <h4 className="font-medium text-sm">Compatible Services</h4>
                  <ul className="text-sm text-muted-foreground space-y-1">
                    <li>• Apache Kafka</li>
                    <li>• Confluent Cloud</li>
                    <li>• AWS MSK</li>
                    <li>• Azure Event Hubs</li>
                  </ul>
                </div>
              </CardContent>
            </Card>

            {kafka.connected && kafka.lineageData && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Cluster Overview</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="grid grid-cols-2 gap-3">
                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                      <div className="text-2xl font-bold text-primary">
                        {kafka.lineageData.topics.length}
                      </div>
                      <div className="text-xs text-muted-foreground">Topics</div>
                    </div>
                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                      <div className="text-2xl font-bold text-ai-primary">
                        {kafka.lineageData.producers.length}
                      </div>
                      <div className="text-xs text-muted-foreground">Producers</div>
                    </div>
                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                      <div className="text-2xl font-bold text-info">
                        {kafka.lineageData.consumers.length}
                      </div>
                      <div className="text-xs text-muted-foreground">Consumers</div>
                    </div>
                    <div className="text-center p-3 bg-muted/50 rounded-lg">
                      <div className="text-2xl font-bold text-success">
                        {kafka.lineageData.connections.filter(c => c.active).length}
                      </div>
                      <div className="text-xs text-muted-foreground">Active Flows</div>
                    </div>
                  </div>
                  
                  <div className="pt-3 border-t">
                    <Link to="/">
                      <Button className="w-full" variant="default">
                        View Live Dashboard
                      </Button>
                    </Link>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default KafkaConfig;