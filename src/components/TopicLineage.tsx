import { useCallback, useMemo, useState, useEffect } from 'react';
import {
  ReactFlow,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  Connection,
  MiniMap,
  Node,
  Edge,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { RefreshCw, ExternalLink, Filter } from "lucide-react";

// Custom node types
const ProducerNode = ({ data }: { data: any }) => (
  <div className="bg-ai-primary/20 border-2 border-ai-primary rounded-lg p-3 min-w-[140px]">
    <div className="text-sm font-medium text-ai-primary">Producer</div>
    <div className="text-xs text-foreground mt-1">{data.label}</div>
    <div className="flex items-center gap-1 mt-2">
      <div className={`w-2 h-2 rounded-full ${data.active ? 'bg-success animate-pulse' : 'bg-muted'}`} />
      <span className="text-xs text-muted-foreground">
        {data.active ? 'Active' : 'Idle'}
      </span>
    </div>
  </div>
);

const TopicNode = ({ data }: { data: any }) => (
  <div className="bg-primary/20 border-2 border-primary rounded-lg p-4 min-w-[160px]">
    <div className="text-sm font-medium text-primary">Topic</div>
    <div className="text-base font-bold text-foreground mt-1">{data.label}</div>
    <div className="grid grid-cols-2 gap-2 mt-2 text-xs">
      <div>
        <span className="text-muted-foreground">Partitions:</span>
        <span className="ml-1 font-medium">{data.partitions}</span>
      </div>
      <div>
        <span className="text-muted-foreground">Size:</span>
        <span className="ml-1 font-medium">{data.size}</span>
      </div>
    </div>
  </div>
);

const ConsumerNode = ({ data }: { data: any }) => (
  <div className="bg-info/20 border-2 border-info rounded-lg p-3 min-w-[140px]">
    <div className="text-sm font-medium text-info">Consumer</div>
    <div className="text-xs text-foreground mt-1">{data.label}</div>
    <div className="flex items-center gap-1 mt-2">
      <div className={`w-2 h-2 rounded-full ${data.active ? 'bg-success animate-pulse' : 'bg-muted'}`} />
      <span className="text-xs text-muted-foreground">
        {data.active ? `Lag: ${data.lag}` : 'Idle'}
      </span>
    </div>
  </div>
);

const nodeTypes = {
  producer: ProducerNode,
  topic: TopicNode,
  consumer: ConsumerNode,
};

// Mock data for topics and their lineage
const generateTopicLineage = () => {
  const topics = [
    { id: 'user-events', label: 'user-events', partitions: 6, size: '2.1GB' },
    { id: 'payment-events', label: 'payment-events', partitions: 3, size: '890MB' },
    { id: 'notification-events', label: 'notification-events', partitions: 2, size: '156MB' },
  ];

  const producers = [
    { id: 'web-app', label: 'Web Application', active: true },
    { id: 'mobile-app', label: 'Mobile App', active: true },
    { id: 'payment-service', label: 'Payment Service', active: true },
    { id: 'user-service', label: 'User Service', active: false },
  ];

  const consumers = [
    { id: 'analytics-consumer', label: 'Analytics Consumer', active: true, lag: '12ms' },
    { id: 'email-service', label: 'Email Service', active: true, lag: '45ms' },
    { id: 'recommendation-engine', label: 'Recommendation Engine', active: false, lag: '0ms' },
    { id: 'audit-service', label: 'Audit Service', active: true, lag: '8ms' },
  ];

  const nodes: Node[] = [
    // Producer nodes
    { 
      id: 'web-app', 
      type: 'producer', 
      position: { x: 50, y: 100 }, 
      data: producers[0] 
    },
    { 
      id: 'mobile-app', 
      type: 'producer', 
      position: { x: 50, y: 200 }, 
      data: producers[1] 
    },
    { 
      id: 'payment-service', 
      type: 'producer', 
      position: { x: 50, y: 300 }, 
      data: producers[2] 
    },
    { 
      id: 'user-service', 
      type: 'producer', 
      position: { x: 50, y: 400 }, 
      data: producers[3] 
    },
    
    // Topic nodes
    { 
      id: 'user-events', 
      type: 'topic', 
      position: { x: 400, y: 150 }, 
      data: topics[0] 
    },
    { 
      id: 'payment-events', 
      type: 'topic', 
      position: { x: 400, y: 300 }, 
      data: topics[1] 
    },
    { 
      id: 'notification-events', 
      type: 'topic', 
      position: { x: 400, y: 450 }, 
      data: topics[2] 
    },
    
    // Consumer nodes
    { 
      id: 'analytics-consumer', 
      type: 'consumer', 
      position: { x: 750, y: 50 }, 
      data: consumers[0] 
    },
    { 
      id: 'email-service', 
      type: 'consumer', 
      position: { x: 750, y: 150 }, 
      data: consumers[1] 
    },
    { 
      id: 'recommendation-engine', 
      type: 'consumer', 
      position: { x: 750, y: 250 }, 
      data: consumers[2] 
    },
    { 
      id: 'audit-service', 
      type: 'consumer', 
      position: { x: 750, y: 350 }, 
      data: consumers[3] 
    },
  ];

  const edges: Edge[] = [
    // Producer to Topic connections
    { 
      id: 'web-user', 
      source: 'web-app', 
      target: 'user-events', 
      animated: true,
      style: { stroke: 'hsl(var(--ai-primary))' },
      label: '2.3k msg/s'
    },
    { 
      id: 'mobile-user', 
      source: 'mobile-app', 
      target: 'user-events', 
      animated: true,
      style: { stroke: 'hsl(var(--ai-primary))' } 
    },
    { 
      id: 'payment-payment', 
      source: 'payment-service', 
      target: 'payment-events', 
      animated: true,
      style: { stroke: 'hsl(var(--ai-primary))' },
      label: '456 msg/s'
    },
    { 
      id: 'user-notification', 
      source: 'user-service', 
      target: 'notification-events', 
      animated: false,
      style: { stroke: 'hsl(var(--muted-foreground))' } 
    },
    
    // Topic to Consumer connections
    { 
      id: 'user-analytics', 
      source: 'user-events', 
      target: 'analytics-consumer', 
      animated: true,
      style: { stroke: 'hsl(var(--info))' },
      label: '2.3k msg/s'
    },
    { 
      id: 'user-email', 
      source: 'user-events', 
      target: 'email-service', 
      animated: true,
      style: { stroke: 'hsl(var(--info))' } 
    },
    { 
      id: 'user-recommendation', 
      source: 'user-events', 
      target: 'recommendation-engine', 
      animated: false,
      style: { stroke: 'hsl(var(--muted-foreground))' } 
    },
    { 
      id: 'payment-audit', 
      source: 'payment-events', 
      target: 'audit-service', 
      animated: true,
      style: { stroke: 'hsl(var(--info))' },
      label: '456 msg/s'
    },
  ];

  return { nodes, edges };
};

export const TopicLineage = () => {
  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => generateTopicLineage(), []);
  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);
  const [refreshing, setRefreshing] = useState(false);
  const [selectedTopic, setSelectedTopic] = useState<string>('all');
  const [allNodes, setAllNodes] = useState(initialNodes);
  const [allEdges, setAllEdges] = useState(initialEdges);

  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  const handleRefresh = () => {
    setRefreshing(true);
    // Simulate refresh
    setTimeout(() => {
      const { nodes: newNodes, edges: newEdges } = generateTopicLineage();
      setAllNodes(newNodes);
      setAllEdges(newEdges);
      filterByTopic(selectedTopic, newNodes, newEdges);
      setRefreshing(false);
    }, 1000);
  };

  const filterByTopic = (topicId: string, nodeList = allNodes, edgeList = allEdges) => {
    if (topicId === 'all') {
      setNodes(nodeList);
      setEdges(edgeList);
      return;
    }

    // Find connected producers and consumers for the selected topic
    const connectedProducers = edgeList
      .filter(edge => edge.target === topicId)
      .map(edge => edge.source);
    
    const connectedConsumers = edgeList
      .filter(edge => edge.source === topicId)
      .map(edge => edge.target);

    // Filter nodes to show only the topic and its connected producers/consumers
    const filteredNodes = nodeList.filter(node => 
      node.id === topicId || 
      connectedProducers.includes(node.id) || 
      connectedConsumers.includes(node.id)
    );

    // Filter edges to show only connections to/from the selected topic
    const filteredEdges = edgeList.filter(edge => 
      edge.source === topicId || edge.target === topicId
    );

    setNodes(filteredNodes);
    setEdges(filteredEdges);
  };

  const handleTopicChange = (value: string) => {
    setSelectedTopic(value);
    filterByTopic(value);
  };

  const handleControlCenter = () => {
    // In a real app, this would open Kafka Control Center
    window.open('http://localhost:9021', '_blank');
  };

  // Simulate real-time updates
  useEffect(() => {
    const interval = setInterval(() => {
      setEdges((eds) => 
        eds.map((edge) => ({
          ...edge,
          animated: edge.animated && Math.random() > 0.3, // Randomly toggle animation
        }))
      );
      
      // Update all edges for filtering consistency
      setAllEdges((eds) => 
        eds.map((edge) => ({
          ...edge,
          animated: edge.animated && Math.random() > 0.3,
        }))
      );
    }, 3000);

    return () => clearInterval(interval);
  }, [setEdges]);

  const topicOptions = [
    { value: 'all', label: 'All Topics' },
    { value: 'user-events', label: 'user-events' },
    { value: 'payment-events', label: 'payment-events' },
    { value: 'notification-events', label: 'notification-events' },
  ];

  return (
    <Card className="h-full bg-card/50 backdrop-blur-sm">
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              Topic Lineage
              <Badge className="bg-success/20 text-success">Live</Badge>
            </CardTitle>
            <p className="text-sm text-muted-foreground mt-1">
              Data flow visualization showing producers, topics, and consumers
            </p>
          </div>
          <div className="flex gap-2">
            <div className="flex items-center gap-2">
              <Filter className="h-4 w-4 text-muted-foreground" />
              <Select value={selectedTopic} onValueChange={handleTopicChange}>
                <SelectTrigger className="w-[180px]">
                  <SelectValue placeholder="Filter by topic" />
                </SelectTrigger>
                <SelectContent>
                  {topicOptions.map((option) => (
                    <SelectItem key={option.value} value={option.value}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={handleRefresh}
              disabled={refreshing}
              className="flex items-center gap-2"
            >
              <RefreshCw className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
            <Button
              variant="default"
              size="sm"
              onClick={handleControlCenter}
              className="flex items-center gap-2 bg-primary hover:bg-primary/80"
            >
              <ExternalLink className="h-4 w-4" />
              Kafka Control Center
            </Button>
          </div>
        </div>
      </CardHeader>
      <CardContent className="h-[600px] p-0">
        <div style={{ width: '100%', height: '100%' }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            nodeTypes={nodeTypes}
            fitView
            attributionPosition="bottom-right"
            style={{ background: 'transparent' }}
            defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
          >
            <Controls 
              style={{
                background: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px'
              }}
            />
            <Background 
              color="hsl(var(--border))" 
              gap={20} 
              size={1}
              style={{ opacity: 0.3 }}
            />
            <MiniMap 
              style={{
                background: 'hsl(var(--card))',
                border: '1px solid hsl(var(--border))',
                borderRadius: '8px'
              }}
              nodeColor={(node) => {
                switch (node.type) {
                  case 'producer': return 'hsl(var(--ai-primary))';
                  case 'topic': return 'hsl(var(--primary))';
                  case 'consumer': return 'hsl(var(--info))';
                  default: return 'hsl(var(--muted))';
                }
              }}
            />
          </ReactFlow>
        </div>
      </CardContent>
    </Card>
  );
};