import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";
import { AlertCircle, CheckCircle, Loader2, Database, Zap } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { UseKafkaReturn } from '@/hooks/useKafka';
import { KafkaConfig } from '@/lib/kafka-client';

interface KafkaConnectionProps {
  kafka: UseKafkaReturn;
}

export const KafkaConnection = ({ kafka }: KafkaConnectionProps) => {
  const [config, setConfig] = useState<KafkaConfig>({
    brokers: ['localhost:9092'],
    clientId: 'kafka-intelligence-platform',
    ssl: false
  });

  const handleConnect = async () => {
    await kafka.connect(config);
  };

  const handleBrokerChange = (index: number, value: string) => {
    const newBrokers = [...config.brokers];
    newBrokers[index] = value;
    setConfig({ ...config, brokers: newBrokers });
  };

  const addBroker = () => {
    setConfig({ ...config, brokers: [...config.brokers, ''] });
  };

  const removeBroker = (index: number) => {
    const newBrokers = config.brokers.filter((_, i) => i !== index);
    setConfig({ ...config, brokers: newBrokers });
  };

  return (
    <Card className="w-full max-w-2xl mx-auto">
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="h-5 w-5" />
          Kafka Cluster Connection
          {kafka.connected && <Badge className="bg-success/20 text-success">Connected</Badge>}
          {kafka.loading && <Badge variant="secondary">Connecting...</Badge>}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-6">
        {kafka.error && (
          <Alert>
            <AlertCircle className="h-4 w-4" />
            <AlertDescription>{kafka.error}</AlertDescription>
          </Alert>
        )}

        <div className="space-y-4">
          <div>
            <Label htmlFor="clientId">Client ID</Label>
            <Input
              id="clientId"
              value={config.clientId}
              onChange={(e) => setConfig({ ...config, clientId: e.target.value })}
              placeholder="kafka-intelligence-platform"
              disabled={kafka.connected}
            />
          </div>

          <div className="space-y-2">
            <Label>Kafka Brokers</Label>
            {config.brokers.map((broker, index) => (
              <div key={index} className="flex gap-2">
                <Input
                  value={broker}
                  onChange={(e) => handleBrokerChange(index, e.target.value)}
                  placeholder="localhost:9092"
                  disabled={kafka.connected}
                />
                {config.brokers.length > 1 && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => removeBroker(index)}
                    disabled={kafka.connected}
                  >
                    Remove
                  </Button>
                )}
              </div>
            ))}
            {!kafka.connected && (
              <Button variant="outline" size="sm" onClick={addBroker}>
                Add Broker
              </Button>
            )}
          </div>

          <Separator />

          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <Label htmlFor="ssl">Enable SSL</Label>
              <Switch
                id="ssl"
                checked={config.ssl}
                onCheckedChange={(checked) => setConfig({ ...config, ssl: checked })}
                disabled={kafka.connected}
              />
            </div>

            {config.ssl && (
              <div className="space-y-2">
                <div>
                  <Label htmlFor="username">Username (Optional)</Label>
                  <Input
                    id="username"
                    value={config.username || ''}
                    onChange={(e) => setConfig({ ...config, username: e.target.value })}
                    placeholder="SASL username"
                    disabled={kafka.connected}
                  />
                </div>
                <div>
                  <Label htmlFor="password">Password (Optional)</Label>
                  <Input
                    id="password"
                    type="password"
                    value={config.password || ''}
                    onChange={(e) => setConfig({ ...config, password: e.target.value })}
                    placeholder="SASL password"
                    disabled={kafka.connected}
                  />
                </div>
              </div>
            )}
          </div>

          <Separator />

          <div className="flex gap-2">
            {!kafka.connected ? (
              <Button
                onClick={handleConnect}
                disabled={kafka.loading || !config.brokers.some(b => b.trim())}
                className="flex items-center gap-2"
              >
                {kafka.loading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Zap className="h-4 w-4" />
                )}
                {kafka.loading ? 'Connecting...' : 'Connect to Kafka'}
              </Button>
            ) : (
              <div className="flex gap-2">
                <Button
                  onClick={kafka.refresh}
                  disabled={kafka.loading}
                  variant="outline"
                  className="flex items-center gap-2"
                >
                  {kafka.loading ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <CheckCircle className="h-4 w-4" />
                  )}
                  Refresh Data
                </Button>
                <Button
                  onClick={kafka.disconnect}
                  variant="destructive"
                >
                  Disconnect
                </Button>
              </div>
            )}
          </div>
        </div>

        {kafka.connected && kafka.lineageData && (
          <div className="mt-6 p-4 bg-muted/50 rounded-lg">
            <h4 className="font-medium mb-2">Connected Cluster Summary</h4>
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Topics:</span>
                <span className="ml-2 font-medium">{kafka.lineageData.topics.length}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Producers:</span>
                <span className="ml-2 font-medium">{kafka.lineageData.producers.length}</span>
              </div>
              <div>
                <span className="text-muted-foreground">Consumers:</span>
                <span className="ml-2 font-medium">{kafka.lineageData.consumers.length}</span>
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};