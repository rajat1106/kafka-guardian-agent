# kafka-guardian-ops MCP server

Exposes the Guardian's Kafka and fleet operations as Model Context Protocol
tools, so the same tool surface the agent uses is available to any MCP client.

## Use from Claude Code

```bash
claude mcp add kafka-guardian -- python /path/to/services/mcp_kafka_ops/server.py
```

## Use from Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "kafka-guardian": {
      "command": "python",
      "args": ["/absolute/path/to/services/mcp_kafka_ops/server.py"],
      "env": {
        "KAFKA_PROVIDER": "local",
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:19092",
        "FLEET_URL": "http://localhost:8081"
      }
    }
  }
}
```

Point `KAFKA_*` at Confluent Cloud and the same tools operate against it —
`describe_cluster` will report that broker-level operations are unavailable.

## Tools

| Tool | Kind | Blast radius |
|---|---|---|
| `describe_cluster` | read | — |
| `list_topics` | read | — |
| `describe_consumer_groups` | read | — |
| `get_service_metrics` | read | — |
| `scale_consumer_group` | mutating | 1 |
| `increase_partitions` | mutating, irreversible | 2 |
| `adjust_connection_pool` | mutating | 2 |
| `restart_service` | mutating, disruptive | 3 |
