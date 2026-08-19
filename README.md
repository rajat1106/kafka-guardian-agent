# Kafka Guardian Agent

An autonomous SRE agent for Apache Kafka. It watches a streaming platform,
predicts failures before they happen, diagnoses the cause, and remediates —
with every action gated by a policy engine that decides what it may do alone
and what needs a human.

Built from the architecture in
[The Future of Self-Healing Infrastructure: Agentic AI + Event Streaming](https://www.linkedin.com/pulse/future-self-healing-infrastructure-agentic-ai-event-streaming-harne-xgjzc/),
rebuilt for 2026.

```
 chaos injector ──inject──▶ demo fleet (payment · user · notification)
                                 │ produce/consume + emit metrics
                                 ▼
 ┌──────────── Kafka 4.x (KRaft, no ZooKeeper) ──────────────┐
 │ telemetry.metrics   telemetry.events                       │
 │ guardian.anomalies  guardian.decisions  guardian.actions   │
 │ guardian.approvals  guardian.outcomes                      │
 └───┬──────────────────────────────────────────────┬─────────┘
     │                                              │
 ┌───▼────────────┐                          ┌──────▼──────┐
 │  Detector      │  no LLM, no tokens       │ API gateway │──WS──▶ dashboard
 │  EWMA z-score  │                          └─────────────┘
 │  IsolationFrst │
 │  trend → ETA   │
 └───┬────────────┘
     │ guardian.anomalies
 ┌───▼────────────────────────────────────────────────────────┐
 │                     Guardian agent                          │
 │  correlate → recall → diagnose → plan → OPA gate            │
 │  → actuate → verify → learn                                 │
 │  step-journaled to Postgres (crash-resumable)               │
 └─────────────────────────────────────────────────────────────┘
```

## Quick start

```bash
git clone https://github.com/rajat1106/kafka-guardian-agent
cd kafka-guardian-agent
cp .env.example .env
docker compose up --build
```

Then open **http://localhost:5173**. Within a couple of minutes the chaos
engine breaks something and you watch the agent handle it.

Four pages, for two audiences:

| Page | For | Shows |
|---|---|---|
| **Overview** | anyone | What the agent did, in sentences. No jargon, no σ. |
| **Topology** | engineers | Live producer → topic → consumer graph with lag and health. |
| **Operations** | on-call | Raw telemetry, detector output, full action audit trail. |
| **Connections** | operators | Plug in your cluster, decision engine and alerting. |

The dashboard leads with a live **topic lineage graph** — producers → topics →
consumer groups, with real partition counts, replica counts, lag, heap and
pool pressure on every node. Nodes light up as the agent acts on them, and a
consumer group sitting at its partition ceiling is marked `cap`, because that
is the constraint the policy engine will refuse to let the agent scale past.

No API key is required. Without one the agent runs its deterministic
planner and the whole demo still works; add a key to enable LLM diagnosis.

## What it actually does

**Predicts rather than reacts.** The detector fits a slope to each metric and
reports seconds-until-breach, so the agent acts before the threshold is
crossed. Measured lead times, from the eval harness:

| scenario | lead time before degradation |
|---|---|
| consumer heap exhaustion (OOM) | 42 s |
| DB pool exhaustion cascade | 26 s |
| consumer lag explosion | 28 s |
| under-replicated partitions | >118 s |
| region outage | 0 s (instantaneous by nature) |

**Diagnoses the cause, not the symptom.** Rising heap with lag is a capacity
shortfall — buffered records for a backlog consume the heap. Rising heap
*without* lag is a suspected leak. Same signal, different cause, different
remedy; the agent distinguishes them.

**Knows what it may not do.** Blast radius is scored 0–5 by a real Rego
policy. Reversible single-service actions execute unattended; anything
reaching a broker or a region parks and waits for a human click. The gate
also encodes Kafka's own rules — it refuses to scale a consumer group beyond
the topic's partition count, because the surplus consumers would sit idle.

**Verifies its own work.** After acting it re-reads live metrics and checks
whether the system actually recovered. A no-op is never recorded as a fix.

## Plug in your own everything

The **Connections** page configures four independent slots at runtime — no
restart, no editing `.env`:

| Slot | Options |
|---|---|
| **Kafka cluster** | Demo (real broker in Docker) · Self-hosted · Confluent Cloud |
| **Decision engine** | Built-in rules engine · Claude (choose triage and deep-analysis models) |
| **Notifications** | Off · Slack · Generic webhook |
| **Extra signals** | Off · Prometheus |

Every slot has a **Save and test connection** button that performs a real
operation — a Kafka metadata request, an actual model call, an actual webhook
POST — and reports what came back. Nothing is marked connected on the strength
of a well-formed input box.

Two rules the UI enforces:

- **Credentials go in and never come back.** Secrets are stored server-side;
  reads return `••••1234`. Retype a field to replace it, leave it to keep it.
- **An untested decision engine is not used.** Configure a Claude key without
  testing it and the agent stays on the rules engine, saying so. An incident
  is the wrong moment to discover a key was wrong.

The **demo cluster** is controlled from the same page: *Start cluster* runs the
actual `docker start`, and alongside the simulated faults there are
container-level ones — freezing or restarting the real broker process, which
produces genuine leader elections and client reconnects that no in-process
simulation can. Container control is scoped in code to the project's own
`kga-` prefix and to five verbs.

### Configuring the cluster by environment instead

`KAFKA_*` in `.env` still works and takes effect at boot:

```bash
KAFKA_PROVIDER=confluent
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_SASL_USERNAME=<CLUSTER_API_KEY>
KAFKA_SASL_PASSWORD=<CLUSTER_API_SECRET>
KAFKA_TOPIC_PREFIX=kga-dev.
```

```bash
docker compose -f docker-compose.yml -f docker-compose.confluent.yml up
```

Security protocol, SASL mechanism and the minimum replication factor of 3 are
applied automatically. More importantly, the agent's **capability model**
changes: broker-level remedies do not exist on a managed cluster, so it stops
proposing them and escalates instead. The eval harness scores this correctly
on both cluster types — `python evals/run_eval.py --provider confluent`.

Kafka admin actions (partition increases, topic config) are real in every
mode. Service-level actions run against the demo fleet by default; set
`ACTUATOR_MODE=k8s` to drive a real Kubernetes cluster instead.

## Token budget

The LLM is optional and tightly bounded, so this stays usable on free-tier
credits. Four controls, in order of how much they save:

1. **Severity floor** — anomalies scoring below `GUARDIAN_LLM_MIN_SEVERITY`
   never reach a model. Most anomalies are minor.
2. **Tiered routing** — Haiku triages; Opus is consulted only for incidents
   triage marks as non-obvious or where a past remedy failed.
3. **Per-incident ceiling** — one pathological incident cannot drain the day.
4. **Rolling hour/day ceilings** — sustained load degrades to the offline
   planner rather than to a surprise bill.

A fully LLM-diagnosed incident costs about **$0.07**. The default daily
ceiling of 400k tokens caps spend near **$2.40/day** worst case. Every limit
degrades to the deterministic planner rather than failing.

## Evaluation

The part most demos skip. Recorded scenarios are replayed through the real
detector and planner and scored offline — no Kafka, no Docker, fast enough
for CI:

```bash
python evals/run_eval.py                    # deterministic planner
python evals/run_eval.py --llm              # LLM planner (needs a key)
python evals/run_eval.py --provider confluent
```

```
scenario            detect     lead  root cause                        action                 grade
consumer_oom           yes     42 s  ok  consumer_capacity_shortfall   scale_consumer_group   correct
pool_exhaustion        yes     26 s  ok  db_connection_pool_exhaustion adjust_db_pool         correct
region_outage          yes      0 s  ok  region_unavailable            failover_region        correct
lag_spike              yes     28 s  ok  consumer_capacity_shortfall   scale_consumer_group   correct
under_replicated       yes   >118 s  ok  partition_replication_degraded roll_broker           correct
  detection rate       5/5
  root-cause accuracy  5/5
  action correct       5/5
  median lead time     35s (max 118s)
  false-positive rate  0.0028  (1 anomaly / 360 healthy observations)
```

It exits non-zero on regression, so it works as a CI gate.

Policy is tested separately with real OPA:

```bash
opa test policy/ -v      # 13 tests
```

## MCP server

The agent's Kafka tools are also exposed as a Model Context Protocol server,
so the same tool surface works in Claude Code or Claude Desktop against your
own cluster:

```bash
claude mcp add kafka-guardian -- python services/mcp_kafka_ops/server.py
```

See [`services/mcp_kafka_ops/README.md`](services/mcp_kafka_ops/README.md).

## Layout

```
platform/        shared contracts, config, Kafka transport
services/
  fleet/         simulated microservices + control API
  chaos/         failure scenarios and injector
  detector/      anomaly detection (no LLM)
  guardian/      the agent: planners, policy, actuators, memory
  mcp_kafka_ops/ MCP server
  api/           read model + WebSocket gateway
policy/          Rego policy + tests
evals/           offline evaluation harness
web/             React dashboard
docs/            architecture and design notes
```

## What changed from the original design

The blog's thesis held up — AI SRE became a real product category. Three
things in its architecture did not, and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
explains the reasoning:

- **RL policy for remediation → LLM + typed tools.** Reinforcement learning
  for choosing remedies did not survive production anywhere.
- **Direct API calls → MCP + a policy gate.** Neither existed in the original.
- **ZooKeeper-era Kafka → KRaft.** ZooKeeper was removed in Kafka 4.0.

Classical ML survived, but moved: Isolation Forest and trend forecasting run
at the detection edge where they are cheap, not in the decision loop.

## Licence

MIT
