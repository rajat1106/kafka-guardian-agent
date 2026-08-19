"""The simulated world.

A self-healing agent is only as convincing as the failures it faces, so
these services are not random-number generators.  Each holds real state
that evolves under load and degrades in a causally correct way:

  * a consumer that falls behind accumulates lag, and lag costs heap,
    and heap exhaustion is what eventually kills it;
  * a connection pool that saturates raises latency *and* error rate,
    and a saturated upstream pushes latency into its callers;
  * a service that dies stops consuming, so its lag climbs while it is
    down and drains once it returns.

That causal structure is what makes the agent's diagnosis a real
inference rather than pattern-matching on a label we handed it.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

from guardian_platform.contracts import ServiceMetrics


@dataclass
class FaultState:
    """Faults currently injected into a service by the chaos engine."""

    memory_leak_mb_per_tick: float = 0.0
    pool_leak_per_tick: float = 0.0
    consumer_slowdown: float = 1.0      # multiplier on processing rate; >1 = slower
    upstream_latency_ms: float = 0.0
    partition_offline: bool = False
    region_down: bool = False
    ttl_ticks: int = 0
    label: str | None = None

    def tick(self) -> None:
        if self.ttl_ticks > 0:
            self.ttl_ticks -= 1
            if self.ttl_ticks == 0:
                self.clear()

    def clear(self) -> None:
        self.memory_leak_mb_per_tick = 0.0
        self.pool_leak_per_tick = 0.0
        self.consumer_slowdown = 1.0
        self.upstream_latency_ms = 0.0
        self.partition_offline = False
        self.region_down = False
        self.label = None

    @property
    def active(self) -> bool:
        return self.label is not None


@dataclass
class SimulatedService:
    """One microservice with believable internal state."""

    name: str
    region: str = "us-east-1"
    heap_limit_mb: float = 1024.0
    db_pool_size: int = 20
    base_request_rate: float = 120.0
    base_latency_ms: float = 45.0
    partition_count: int = 6
    consumer_group: str | None = None
    replicas: int = 2

    # Mutable state
    heap_used_mb: float = 240.0
    consumer_lag: int = 0
    db_pool_used: int = 0
    healthy: bool = True
    restarting_ticks: int = 0
    throttle_factor: float = 1.0
    faults: FaultState = field(default_factory=FaultState)
    # Leaked connections are tracked separately from demand-driven usage;
    # folding them into db_pool_used makes the pool compound into its own
    # demand and every service saturates on the first tick.
    _pool_leak: float = 0.0
    _t: int = 0

    # ── lifecycle ────────────────────────────────────────────────
    def restart(self) -> None:
        """Cold restart: clears heap and lag debt, costs two ticks of downtime."""
        self.heap_used_mb = 240.0
        self.db_pool_used = 0
        self._pool_leak = 0.0
        self.restarting_ticks = 2
        self.healthy = False
        self.faults.clear()

    def scale(self, replicas: int) -> None:
        self.replicas = max(1, min(replicas, 20))

    def resize_pool(self, size: int) -> None:
        self.db_pool_size = max(5, min(size, 200))
        # Resizing the pool recycles it, which is why it relieves a leak.
        self._pool_leak = 0.0

    def add_partitions(self, count: int) -> None:
        # Mirrors Kafka: partition count can only increase.
        self.partition_count = max(self.partition_count, count)

    def clear_backlog(self) -> None:
        self.consumer_lag = int(self.consumer_lag * 0.1)

    def throttle(self, factor: float) -> None:
        self.throttle_factor = max(0.1, min(factor, 1.0))

    # ── per-tick physics ─────────────────────────────────────────
    def step(self) -> ServiceMetrics:
        self._t += 1
        self.faults.tick()

        if self.restarting_ticks > 0:
            self.restarting_ticks -= 1
            if self.restarting_ticks == 0:
                self.healthy = True

        # Diurnal-ish load curve plus noise, so baselines are not flat.
        wave = 1.0 + 0.18 * math.sin(self._t / 24.0)
        request_rate = self.base_request_rate * wave * self.throttle_factor
        request_rate *= random.uniform(0.94, 1.06)
        if not self.healthy:
            request_rate = 0.0

        # ── consumer lag ─────────────────────────────────────────
        # Throughput scales with replicas and partitions (you cannot use
        # more consumers than partitions — the real Kafka constraint).
        effective_consumers = min(self.replicas, self.partition_count)
        capacity = effective_consumers * 95.0 / max(self.faults.consumer_slowdown, 0.01)
        if not self.healthy:
            capacity = 0.0
        backlog_delta = request_rate - capacity
        self.consumer_lag = max(0, int(self.consumer_lag + backlog_delta))

        # ── heap ─────────────────────────────────────────────────
        # Baseline working set, plus buffered records for the backlog,
        # plus whatever the injected leak adds. This is why an OOM is
        # *predictable*: lag growth leads heap growth.
        buffered = self.consumer_lag * 0.012
        self.heap_used_mb += self.faults.memory_leak_mb_per_tick
        target = 240.0 + buffered + (self.heap_used_mb - 240.0)
        self.heap_used_mb = max(180.0, min(target, self.heap_limit_mb * 1.05))
        if self.heap_used_mb >= self.heap_limit_mb:
            # OOM kill — the thing the agent is supposed to prevent.
            self.healthy = False
            self.restarting_ticks = 3
            self.heap_used_mb = 240.0

        # ── connection pool ──────────────────────────────────────
        demand = request_rate / 22.0
        self._pool_leak = max(0.0, self._pool_leak + self.faults.pool_leak_per_tick)
        self.db_pool_used = max(0.0, min(demand + self._pool_leak, self.db_pool_size))
        utilisation = self.db_pool_used / max(self.db_pool_size, 1)

        # ── latency & errors ─────────────────────────────────────
        # Pool saturation is super-linear: the last 15% of a pool is where
        # queuing blows up, which is what makes the cascade believable.
        saturation_penalty = 0.0
        if utilisation > 0.85:
            saturation_penalty = ((utilisation - 0.85) / 0.15) ** 2 * 900.0
        lag_penalty = min(self.consumer_lag / 60.0, 400.0)
        latency = (
            self.base_latency_ms
            + saturation_penalty
            + lag_penalty
            + self.faults.upstream_latency_ms
            + random.uniform(-4, 6)
        )
        error_rate = 0.001
        if utilisation > 0.95:
            error_rate = min(0.4, (utilisation - 0.95) / 0.05 * 0.35)
        if not self.healthy:
            error_rate = 1.0
            latency = 0.0

        cpu = min(97.0, 22.0 + request_rate / 6.0 + saturation_penalty / 40.0
                  + random.uniform(-3, 3))

        return ServiceMetrics(
            service=self.name,
            consumer_lag=self.consumer_lag,
            messages_per_sec=round(request_rate, 2),
            partition_count=self.partition_count,
            under_replicated_partitions=3 if self.faults.partition_offline else 0,
            memory_used_pct=round(self.heap_used_mb / self.heap_limit_mb * 100, 2),
            cpu_pct=round(max(0.0, cpu), 2),
            heap_used_mb=round(self.heap_used_mb, 2),
            db_pool_used=int(self.db_pool_used),
            db_pool_size=self.db_pool_size,
            p99_latency_ms=round(max(0.0, latency), 2),
            error_rate=round(error_rate, 4),
            request_rate=round(request_rate, 2),
            healthy=self.healthy and not self.faults.region_down,
            region=self.region,
        )


def build_fleet() -> dict[str, SimulatedService]:
    """The three services from the original blog post's scenarios."""
    return {
        "payment-service": SimulatedService(
            name="payment-service", heap_limit_mb=1024, db_pool_size=20,
            base_request_rate=140, base_latency_ms=52, partition_count=6,
            consumer_group="payment-processors", replicas=2,
        ),
        "user-service": SimulatedService(
            name="user-service", heap_limit_mb=768, db_pool_size=30,
            base_request_rate=210, base_latency_ms=38, partition_count=6,
            consumer_group="user-events-consumers", replicas=3,
        ),
        "notification-service": SimulatedService(
            name="notification-service", heap_limit_mb=512, db_pool_size=15,
            base_request_rate=85, base_latency_ms=61, partition_count=3,
            consumer_group="notification-workers", replicas=2,
        ),
    }
