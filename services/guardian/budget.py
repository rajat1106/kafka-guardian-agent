"""Token budget — a circuit breaker, not a counter.

The requirement is that this system stays usable on free-tier credits.
A metrics pipeline emitting every two seconds will happily burn a month's
allowance in an afternoon if the agent is allowed to think about every
blip, so the budget is enforced at four levels:

1. **Severity floor** — anomalies below a score threshold never reach the
   LLM at all. This is the cheapest and most effective control: most
   anomalies are minor and the offline planner handles them correctly.
2. **Model routing** — a cheap model triages; the expensive model is only
   consulted for incidents triage marks as non-obvious.
3. **Per-incident ceiling** — one pathological incident cannot drain the
   day's budget.
4. **Rolling hour/day ceilings** — sustained load degrades to offline
   planning rather than to a surprise invoice.

When any ceiling is hit the agent does not stop working; it falls back to
the deterministic planner. Degrade, never fail.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Literal

# USD per million tokens. Keep in sync with the pricing page; used for the
# cost figures shown in the dashboard and the eval report.
PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-5": (5.00, 25.00),
    "claude-sonnet-5": (3.00, 15.00),
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-opus-4-8": (5.00, 25.00),
    "claude-sonnet-4-6": (3.00, 15.00),
}
_FALLBACK_PRICE = (5.00, 25.00)


def cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    inp, out = PRICING.get(model, _FALLBACK_PRICE)
    return (input_tokens * inp + output_tokens * out) / 1_000_000


@dataclass(frozen=True)
class Spend:
    ts: float
    model: str
    input_tokens: int
    output_tokens: int
    incident_id: str

    @property
    def total(self) -> int:
        return self.input_tokens + self.output_tokens

    @property
    def usd(self) -> float:
        return cost_usd(self.model, self.input_tokens, self.output_tokens)


Verdict = Literal["ok", "incident_exhausted", "hour_exhausted", "day_exhausted", "severity_floor"]


@dataclass
class BudgetStatus:
    verdict: Verdict
    detail: str
    incident_used: int = 0
    incident_limit: int = 0
    hour_used: int = 0
    hour_limit: int = 0
    day_used: int = 0
    day_limit: int = 0
    day_cost_usd: float = 0.0

    @property
    def allowed(self) -> bool:
        return self.verdict == "ok"


class TokenBudget:
    """Thread-safe rolling token budget."""

    def __init__(
        self,
        per_incident: int,
        per_hour: int,
        per_day: int,
        min_severity: float,
    ) -> None:
        self.per_incident = per_incident
        self.per_hour = per_hour
        self.per_day = per_day
        self.min_severity = min_severity
        self._spends: Deque[Spend] = deque()
        self._per_incident_used: dict[str, int] = {}
        self._lock = threading.Lock()

    # ── bookkeeping ──────────────────────────────────────────────────
    def _prune(self, now: float) -> None:
        cutoff = now - 86_400
        while self._spends and self._spends[0].ts < cutoff:
            self._spends.popleft()

    def _window_total(self, now: float, seconds: float) -> int:
        cutoff = now - seconds
        return sum(s.total for s in self._spends if s.ts >= cutoff)

    def record(self, spend: Spend) -> None:
        with self._lock:
            self._spends.append(spend)
            self._per_incident_used[spend.incident_id] = (
                self._per_incident_used.get(spend.incident_id, 0) + spend.total
            )
            self._prune(spend.ts)

    # ── gating ───────────────────────────────────────────────────────
    def check(
        self,
        incident_id: str,
        severity_score: float | None = None,
        estimated_tokens: int = 0,
    ) -> BudgetStatus:
        """Decide whether the LLM path may be used for this incident."""
        now = time.time()
        with self._lock:
            self._prune(now)
            incident_used = self._per_incident_used.get(incident_id, 0)
            hour_used = self._window_total(now, 3_600)
            day_used = self._window_total(now, 86_400)
            day_cost = sum(
                s.usd for s in self._spends if s.ts >= now - 86_400
            )

        base = dict(
            incident_used=incident_used, incident_limit=self.per_incident,
            hour_used=hour_used, hour_limit=self.per_hour,
            day_used=day_used, day_limit=self.per_day,
            day_cost_usd=round(day_cost, 4),
        )

        # The cheapest control first: never spend tokens on a minor anomaly.
        if severity_score is not None and severity_score < self.min_severity:
            return BudgetStatus(
                verdict="severity_floor",
                detail=(f"anomaly score {severity_score:.2f} is below the LLM "
                        f"floor of {self.min_severity:.2f}; handled offline"),
                **base,
            )
        if incident_used + estimated_tokens > self.per_incident:
            return BudgetStatus(
                verdict="incident_exhausted",
                detail=(f"incident has used {incident_used} of "
                        f"{self.per_incident} tokens"),
                **base,
            )
        if day_used + estimated_tokens > self.per_day:
            return BudgetStatus(
                verdict="day_exhausted",
                detail=f"daily budget exhausted ({day_used}/{self.per_day})",
                **base,
            )
        if hour_used + estimated_tokens > self.per_hour:
            return BudgetStatus(
                verdict="hour_exhausted",
                detail=f"hourly budget exhausted ({hour_used}/{self.per_hour})",
                **base,
            )
        return BudgetStatus(verdict="ok", detail="within budget", **base)

    def remaining_for_incident(self, incident_id: str) -> int:
        with self._lock:
            return max(0, self.per_incident - self._per_incident_used.get(incident_id, 0))

    def snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            self._prune(now)
            hour = self._window_total(now, 3_600)
            day = self._window_total(now, 86_400)
            cost = sum(s.usd for s in self._spends if s.ts >= now - 86_400)
            calls = len(self._spends)
        return {
            "hour_used": hour, "hour_limit": self.per_hour,
            "day_used": day, "day_limit": self.per_day,
            "day_cost_usd": round(cost, 4),
            "llm_calls_24h": calls,
            "per_incident_limit": self.per_incident,
            "min_severity": self.min_severity,
        }
