# Architecture and design decisions

This document explains *why* the system is shaped the way it is, including
the places where the original 2024/25 design was deliberately changed and the
places where a simpler choice was made than the "correct" production one.

## The eight-step loop

```
correlate → recall → diagnose → plan → gate → execute → verify → learn
```

Each step exists because removing it produces a specific failure:

| Step | Removing it causes |
|---|---|
| correlate | one fault opens six incidents, one per metric that moved |
| recall | the agent re-derives the same conclusion every time and never learns a remedy failed |
| diagnose | you act on symptoms — restarting a pod whose heap will refill in 40s |
| plan | untyped actions the policy engine cannot reason about |
| gate | an agent with unbounded blast radius, which nobody will run in production |
| execute | a very expensive dashboard |
| verify | "success" means the API call was accepted, not that anything recovered |
| learn | no improvement over time and no data to evaluate against |

## Decisions

### Detection is statistical, not learned end-to-end

The detector runs EWMA/robust-z, Isolation Forest and a linear trend forecast.
No LLM. Metrics arrive every two seconds per service; sending that to a model
would cost more than the incidents are worth and add latency to the one part
of the system that must be fast.

Median/MAD is used instead of mean/stdev because a single large spike inflates
the standard deviation and masks the very anomaly that produced it.

The trend forecast is what earns the phrase "self-healing": it reports
seconds-until-breach so the agent acts before the threshold is crossed. This
had a subtle consequence during development — the first version of the
diagnoser only tested *current* values, so a pool predicted to breach in 78
seconds was still at 44% and got classified as `unclassified_anomaly`. The
prediction was correct and then discarded. Diagnosis rules must consult the
projection, not just the level.

### The decision layer is an LLM with typed tools, not reinforcement learning

The original design used an RL policy to select remediations. That approach
did not survive contact with production anywhere in the industry, for three
reasons: reward design for "did the infrastructure get better" is unsolved,
sample efficiency is hopeless when each episode is a real outage, and an
unexplainable policy touching production infrastructure is not something an
on-call engineer will approve.

What replaced it is a model that reasons over evidence and emits a *typed*
action from a closed set. Typing matters more than the model: the moment a
plan becomes "run this command", blast radius becomes unknowable and the
policy engine has nothing to evaluate.

### The policy gate is real Rego, and it fails closed

Every mutating action is evaluated by OPA before it reaches an actuator.
The policy assigns blast radius 0–5 and returns `allow`, `require_approval`
or `deny`.

Two properties are deliberate:

- **Unknown actions are denied with maximum blast radius.** The first version
  used one Rego rule per action type, which left `blast_radius` *undefined*
  for an unrecognised action — and an undefined value made the entire
  decision object undefined, so the agent saw no denial at all. A lookup
  table with a `default` fixes it. Fail-safe defaults have to be tested for,
  not assumed.
- **An unreachable policy engine is a denial.** Unavailable policy means
  unknown blast radius, and executing an action of unknown blast radius
  unattended is exactly what this layer exists to prevent.

The policy also encodes Kafka's own semantics — refusing to scale a consumer
group beyond the topic's partition count, because the extra consumers idle.
That rule prevents a plausible-looking remediation loop where the agent
"fixes" lag repeatedly by adding pods that do no work.

### Capability is configuration, not a runtime error

A managed cluster has no broker-level operations. Discovering that when an
actuator throws is too late — the agent has already committed to a plan.
So `ClusterCapabilities` is derived from the provider at config time, passed
into the planner *and* re-checked by the policy.

The observable result: the same under-replicated-partition incident produces
`roll_broker` on a self-hosted cluster and `no_op` + escalate on Confluent
Cloud. The eval harness grades both as correct, because correctness here is
cluster-dependent.

### Durability: a step journal, not a workflow engine

Remediation is long-running and actions are not safely repeatable. Scaling a
consumer group twice because the process died between executing and recording
is the class of bug that destroys trust in automation.

The production-correct answer is a durable execution engine — Temporal,
Restate, or Vercel Workflow. This uses a Postgres step journal instead: the
agent writes each completed step before moving on and skips journaled steps
on resume.

**This is a deliberate simplification.** It gives crash-resumability without a
second stateful service, which suits a demo that must start with one command.
It does *not* give you retries with backoff, timers that survive restarts, or
visibility into in-flight workflows. To swap: replace `_run_incident` in
`services/guardian/agent.py` with a Temporal workflow and each `_journal` call
becomes an activity boundary. The step names already map one-to-one.

### Memory is structural, not semantic

Past incidents are matched by signature — service plus the set of metrics that
fired — rather than by embedding similarity. At this scale that is exact,
free, and needs no vector store. Two incidents are "the same kind of thing"
when the same signals move on the same service, which is a better match
criterion than prose similarity between generated summaries.

Recall also *saves* tokens: a matched past incident usually lets the cheap
triage model settle the case without escalating.

### Cost control is a circuit breaker, not a counter

Four layers, in order of effectiveness: a severity floor that keeps minor
anomalies away from any model at all, tiered model routing, a per-incident
ceiling, and rolling hour/day ceilings.

Every limit degrades to the deterministic planner rather than failing. An
agent that stops working when the budget runs out is worse than one that gets
slightly less insightful.

Note that cache-read and cache-creation tokens are counted toward the
ceilings. They are billed at a lower rate, but treating them as free is how
budgets get blown.

### The offline planner is a first-class path, not a stub

Without an API key — and whenever budget is exhausted — an expert system
handles the incident. It is genuinely good: it scores 5/5 on the eval suite.

It is also honestly limited. It encodes the causal structure of failures
someone anticipated. It cannot explain a failure mode nobody wrote a rule
for, which is precisely the gap the LLM fills. Both paths are kept because
they fail differently.

## Deliberate omissions

- **No Temporal.** See above; the swap is documented.
- **No vector database.** Structural matching is better here and free.
- **No RL.** Kept out on purpose rather than shipped as a token gesture.
- **No multi-agent orchestration.** One agent with good tools beats several
  agents negotiating. Fan-out would help if diagnosis needed to read many
  independent sources; it does not.

## Verified vs unverified

Honest status at the time of writing:

**Verified by execution:** the detector's lead times and false-positive rate;
all 13 Rego policy tests against real OPA; the full loop end-to-end in Docker
(detect → diagnose → gate → act → verify → close), including autonomous
resolution of the OOM and pool-exhaustion scenarios; the human approval gate,
including that an approval resumes a parked plan; the offline planner on all
five scenarios against both cluster capability sets; the dashboard against
live data.

Several bugs were only visible from running the whole system, and are worth
recording because unit tests could not have caught them:

- The diagnoser initially read only *current* metric values, so a pool
  predicted to breach in 78 s was still at 44% and classified as
  unclassifiable. The prediction was correct and then discarded.
- A no-op plan carries no verification metric, and the "no metric → assume
  fine" branch sat ahead of the no-op check, so every escalation was scored
  as a resolved incident.
- Importing a helper from `services/detector` into the guardian worked on a
  developer machine and crash-looped in the container. `ops/import_check.py`
  now reproduces container import paths exactly.
- Editing `plan()` by text-slice silently deleted two `cause` branches; the
  agent kept diagnosing correctly and quietly did nothing.
  `ops/planner_check.py` now asserts the cause→plan mapping is total.
- The simulation accumulated consumer lag once per tick using a per-second
  rate, so lag grew at half its stated speed. Any consumer of those metrics
  that did the arithmetic correctly then under-provisioned.
- Sizing a consumer group to just above the arrival rate stops a backlog
  growing but leaves it draining for minutes. Remedies are now sized for a
  target drain time *and* headroom against a fluctuating arrival rate.

**Not verified by execution:** the LLM planner's live path — no API key was
available in the build environment. Its budget gating, fallback behaviour,
prompt construction and schema are tested; the network round-trip is not.
The Kubernetes actuator is likewise written against the documented API but
has not been run against a cluster.

**Known limitation:** the LLM system prompt and action catalogue are marked
with `cache_control`, but their combined prefix is ~760 tokens — below the
~1024-token minimum cacheable prefix. Caching does not currently engage.
The markers are harmless and will activate if the catalogue grows.
