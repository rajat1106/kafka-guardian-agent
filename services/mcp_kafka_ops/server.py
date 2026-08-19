"""MCP server exposing Kafka + fleet operations as standard tools.

Two reasons this exists rather than the agent calling functions directly:

1. **Reuse.** These tools work in Claude Desktop, Claude Code, or any other
   MCP client. Point one at a production cluster and you have an interactive
   Kafka assistant without writing a second integration.
2. **Boundary.** MCP forces the tool surface to be declared with schemas and
   descriptions rather than being implicit in Python signatures, which is
   what makes the read/write split reviewable.

Read-only tools are unrestricted. Every mutating tool states in its own
description that it is policy-gated, because a tool description is the only
thing a model reads before choosing to call it.
"""

from __future__ import annotations

import json
import os
import sys

import httpx
from mcp.server.fastmcp import FastMCP

sys.path.insert(0, "/app/platform")

from guardian_platform.config import kafka_settings  # noqa: E402
from guardian_platform.kafka import client_kwargs  # noqa: E402

FLEET_URL = os.getenv("FLEET_URL", "http://fleet:8081").rstrip("/")

mcp = FastMCP("kafka-guardian-ops")


# ── read-only ────────────────────────────────────────────────────────
@mcp.tool()
async def describe_cluster() -> str:
    """Describe the Kafka cluster this agent is attached to, including which
    administrative operations are available. Managed clusters such as
    Confluent Cloud do not permit broker-level operations; check here before
    proposing any remediation that touches a broker."""
    ks = kafka_settings()
    return json.dumps({
        "provider": ks.provider.value,
        "description": ks.describe(),
        "bootstrap_servers": ks.bootstrap_servers,
        "security_protocol": ks.security_protocol,
        "replication_factor": ks.replication_factor,
        "topic_prefix": ks.topic_prefix or "(none)",
        "capabilities": ks.capabilities.model_dump(),
    }, indent=2)


@mcp.tool()
async def list_topics() -> str:
    """List every topic on the cluster with its partition count and replication
    factor. Use this to check the partition ceiling before proposing to scale
    a consumer group, since consumers beyond the partition count sit idle."""
    from aiokafka.admin import AIOKafkaAdminClient

    ks = kafka_settings()
    admin = AIOKafkaAdminClient(client_id="mcp-kafka-ops", **client_kwargs(ks))
    await admin.start()
    try:
        names = await admin.list_topics()
        described = await admin.describe_topics(list(names))
        out = [
            {
                "topic": t["topic"],
                "partitions": len(t["partitions"]),
                "replication_factor": (
                    len(t["partitions"][0]["replicas"]) if t["partitions"] else 0
                ),
            }
            for t in described
        ]
        return json.dumps(sorted(out, key=lambda x: x["topic"]), indent=2)
    finally:
        await admin.close()


@mcp.tool()
async def describe_consumer_groups() -> str:
    """List consumer groups and their state. A group in a state other than
    'Stable' is rebalancing, which itself causes lag and should not be
    mistaken for a capacity shortfall."""
    from aiokafka.admin import AIOKafkaAdminClient

    ks = kafka_settings()
    admin = AIOKafkaAdminClient(client_id="mcp-kafka-ops", **client_kwargs(ks))
    await admin.start()
    try:
        groups = await admin.list_consumer_groups()
        return json.dumps(
            [{"group_id": g[0], "protocol_type": g[1]} for g in groups], indent=2
        )
    finally:
        await admin.close()


@mcp.tool()
async def get_service_metrics(service: str = "") -> str:
    """Read current metrics for one service, or all services when `service` is
    empty. Returns consumer lag, heap use, p99 latency, error rate, connection
    pool utilisation, replica count and partition count."""
    async with httpx.AsyncClient(timeout=8) as c:
        r = await c.get(f"{FLEET_URL}/services")
        r.raise_for_status()
        data = r.json()
    if service:
        if service not in data:
            return json.dumps({"error": f"unknown service {service!r}",
                               "known": sorted(data)})
        return json.dumps({service: data[service]}, indent=2)
    return json.dumps(data, indent=2)


# ── mutating (policy-gated) ──────────────────────────────────────────
@mcp.tool()
async def scale_consumer_group(service: str, replicas: int) -> str:
    """Change the number of consumer replicas for a service.

    POLICY-GATED: evaluated against the remediation policy before execution
    and rejected if replicas exceed the topic's partition count, because the
    surplus consumers would be idle. Blast radius 1 (reversible, one service).
    """
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{FLEET_URL}/services/{service}/scale",
                         json={"replicas": replicas})
        r.raise_for_status()
        return json.dumps(r.json())


@mcp.tool()
async def increase_partitions(topic: str, partitions: int) -> str:
    """Increase a topic's partition count on the live cluster.

    POLICY-GATED. IRREVERSIBLE: Kafka partition counts can only increase.
    Raising partitions also changes key-to-partition mapping, so strict
    per-key ordering is broken for existing keys. Blast radius 2.
    """
    from aiokafka.admin import AIOKafkaAdminClient, NewPartitions

    ks = kafka_settings()
    admin = AIOKafkaAdminClient(client_id="mcp-kafka-ops", **client_kwargs(ks))
    await admin.start()
    try:
        await admin.create_partitions(
            {ks.topic(topic): NewPartitions(total_count=partitions)}
        )
        return json.dumps({"topic": ks.topic(topic), "partitions": partitions,
                           "cluster": ks.describe()})
    finally:
        await admin.close()


@mcp.tool()
async def adjust_connection_pool(service: str, size: int) -> str:
    """Resize a service's database connection pool, recycling leaked
    connections in the process.

    POLICY-GATED. Blast radius 2 (affects the service's clients briefly).
    """
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{FLEET_URL}/services/{service}/pool", json={"size": size})
        r.raise_for_status()
        return json.dumps(r.json())


@mcp.tool()
async def restart_service(service: str) -> str:
    """Cold-restart a service. Clears heap and in-flight state at the cost of
    a short outage.

    POLICY-GATED and DISRUPTIVE: blast radius 3, so it requires human
    approval under the default autonomy setting. Restarting clears symptoms
    of a backlog without addressing the cause, so prefer a capacity change
    when lag is the driver.
    """
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{FLEET_URL}/services/{service}/restart")
        r.raise_for_status()
        return json.dumps(r.json())


if __name__ == "__main__":
    # stdio transport: usable directly from Claude Desktop / Claude Code.
    mcp.run(transport="stdio")
