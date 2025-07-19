import { useState, useEffect, useCallback } from 'react';
import { KafkaClient, KafkaConfig, LineageData, defaultKafkaConfig } from '@/lib/kafka-client';

export interface UseKafkaReturn {
  client: KafkaClient | null;
  connected: boolean;
  loading: boolean;
  error: string | null;
  lineageData: LineageData | null;
  connect: (config?: KafkaConfig) => Promise<void>;
  disconnect: () => void;
  refresh: () => Promise<void>;
}

export const useKafka = (): UseKafkaReturn => {
  const [client, setClient] = useState<KafkaClient | null>(null);
  const [connected, setConnected] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lineageData, setLineageData] = useState<LineageData | null>(null);

  const connect = useCallback(async (config: KafkaConfig = defaultKafkaConfig) => {
    setLoading(true);
    setError(null);

    try {
      const kafkaClient = new KafkaClient(config);
      await kafkaClient.connect();
      
      setClient(kafkaClient);
      setConnected(true);

      // Get initial data
      const data = await kafkaClient.getLineageData();
      setLineageData(data);

      // Subscribe to real-time updates
      const unsubscribe = kafkaClient.subscribeToUpdates((updatedData) => {
        setLineageData(prev => ({ ...prev, ...updatedData } as LineageData));
      });

      // Store unsubscribe function for cleanup
      (kafkaClient as any).unsubscribe = unsubscribe;

    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to connect to Kafka');
      setConnected(false);
    } finally {
      setLoading(false);
    }
  }, []);

  const disconnect = useCallback(() => {
    if (client) {
      if ((client as any).unsubscribe) {
        (client as any).unsubscribe();
      }
      client.disconnect();
      setClient(null);
      setConnected(false);
      setLineageData(null);
    }
  }, [client]);

  const refresh = useCallback(async () => {
    if (!client) return;

    setLoading(true);
    try {
      const data = await client.getLineageData();
      setLineageData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to refresh data');
    } finally {
      setLoading(false);
    }
  }, [client]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      disconnect();
    };
  }, [disconnect]);

  return {
    client,
    connected,
    loading,
    error,
    lineageData,
    connect,
    disconnect,
    refresh
  };
};