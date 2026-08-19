"""Anomaly detection — the part of the system that must never call an LLM.

Three complementary detectors, because they fail in different ways:

* **Robust z-score** catches sudden level shifts. It uses median/MAD rather
  than mean/stdev: with mean/stdev a single huge spike inflates the stdev
  and masks the very anomaly that produced it.
* **Isolation Forest** catches combinations that are individually normal but
  jointly strange — moderate lag *and* moderate heap *and* rising latency is
  the signature of the OOM scenario long before any single metric alarms.
* **Trend forecast** is the one that earns the word "self-healing": it fits a
  slope to the recent window and reports seconds-until-breach, so the agent
  can act before the threshold is crossed rather than after.

Everything here is deterministic and cheap. The LLM is only woken for
anomalies that clear a severity floor, which is what keeps the token bill
bounded on a system emitting metrics every two seconds.
"""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Iterable

from guardian_platform.contracts import Anomaly, ServiceMetrics, Severity
from guardian_platform.stats import linear_slope, median as _median, robust_z

# Metrics the detectors watch, with the ceiling each one is racing toward.
# `None` means the metric has no fixed ceiling and is judged only relative
# to its own history.
WATCHED: dict[str, float | None] = {
    "consumer_lag": 5000.0,
    "memory_used_pct": 92.0,
    "p99_latency_ms": 800.0,
    "error_rate": 0.10,
    "cpu_pct": 90.0,
    "db_pool_utilisation": 0.90,
    "under_replicated_partitions": 1.0,
}

# Below this many samples a series is considered still warming up. Flagging
# during warm-up is how naive detectors produce a burst of false positives
# every time the system restarts.
# Metrics that corroborate a diagnosis but must never open an incident on
# their own. High CPU with nothing else moving has no actionable remedy —
# alerting on it produces incidents whose only honest outcome is "no action
# taken", which trains everyone to ignore the agent. They still feed the
# multivariate detector and the diagnosis context.
CORROBORATING_ONLY = {"cpu_pct"}

# Below this many samples a series is considered still warming up.
MIN_SAMPLES = 12
# A trend needs more history than a level shift. On a cold start every metric
# is ramping from zero toward its steady state, and a slope fitted to that
# ramp extrapolates to a breach that will never happen.
MIN_SAMPLES_TREND = 30
WINDOW = 60


def _extract(m: ServiceMetrics) -> dict[str, float]:
    return {
        "consumer_lag": float(m.consumer_lag),
        "memory_used_pct": m.memory_used_pct,
        "p99_latency_ms": m.p99_latency_ms,
        "error_rate": m.error_rate,
        "cpu_pct": m.cpu_pct,
        "db_pool_utilisation": m.db_pool_utilisation,
        "under_replicated_partitions": float(m.under_replicated_partitions),
    }


@dataclass
class SeriesState:
    values: Deque[float] = field(default_factory=lambda: deque(maxlen=WINDOW))
    # Suppresses re-alerting on every tick of the same ongoing anomaly.
    cooldown: int = 0


class IsolationForestDetector:
    """Multivariate outlier scoring.

    Uses scikit-learn when present and degrades to a Mahalanobis-style
    distance otherwise, so the image stays runnable without a 90 MB
    dependency if someone wants a slim build.
    """

    def __init__(self, min_samples: int = 40, refit_every: int = 30) -> None:
        self._history: dict[str, Deque[list[float]]] = defaultdict(
            lambda: deque(maxlen=400)
        )
        self._models: dict[str, object] = {}
        self._since_fit: dict[str, int] = defaultdict(int)
        self._min_samples = min_samples
        self._refit_every = refit_every
        try:
            from sklearn.ensemble import IsolationForest  # noqa: F401
            self._available = True
        except ImportError:
            self._available = False

    @property
    def backend(self) -> str:
        return "sklearn" if self._available else "mahalanobis"

    def observe(self, service: str, features: list[float]) -> float | None:
        """Return an outlier score in 0..1, or None while warming up."""
        hist = self._history[service]
        hist.append(features)
        if len(hist) < self._min_samples:
            return None

        if self._available:
            return self._sklearn_score(service, features)
        return self._fallback_score(service, features)

    def _sklearn_score(self, service: str, features: list[float]) -> float:
        from sklearn.ensemble import IsolationForest

        self._since_fit[service] += 1
        if service not in self._models or self._since_fit[service] >= self._refit_every:
            model = IsolationForest(
                n_estimators=80, contamination=0.06, random_state=7
            )
            model.fit(list(self._history[service]))
            self._models[service] = model
            self._since_fit[service] = 0
        model = self._models[service]
        # decision_function: positive = inlier, negative = outlier.
        raw = float(model.decision_function([features])[0])
        return max(0.0, min(1.0, 0.5 - raw))

    def _fallback_score(self, service: str, features: list[float]) -> float:
        hist = list(self._history[service])
        dims = len(features)
        score = 0.0
        for d in range(dims):
            col = [row[d] for row in hist]
            med = _median(col)
            mad = _median([abs(x - med) for x in col]) or 1e-6
            score = max(score, min(1.0, abs(features[d] - med) * 0.6745 / mad / 8.0))
        return score


class Detector:
    """Stateful per-service, per-metric anomaly detection."""

    def __init__(self) -> None:
        self._series: dict[tuple[str, str], SeriesState] = defaultdict(SeriesState)
        self._forest = IsolationForestDetector()

    @property
    def forest_backend(self) -> str:
        return self._forest.backend

    def observe(self, m: ServiceMetrics) -> list[Anomaly]:
        found: list[Anomaly] = []
        values = _extract(m)

        # A service that is down produces zeros everywhere; those zeros are a
        # consequence of the outage, not seven independent anomalies. Report
        # the outage once and skip the univariate pass.
        if not m.healthy:
            key = (m.service, "healthy")
            state = self._series[key]
            if state.cooldown == 0:
                state.cooldown = 8
                found.append(Anomaly(
                    service=m.service, metric="healthy", value=0.0, baseline=1.0,
                    deviation_sigma=99.0, severity=Severity.CRITICAL, score=0.99,
                    detector="threshold",
                    description=f"{m.service} is reporting unhealthy in {m.region}",
                ))
            else:
                state.cooldown -= 1
            for k in values:
                self._series[(m.service, k)].values.append(values[k])
            return found

        for metric, value in values.items():
            state = self._series[(m.service, metric)]
            window = list(state.values)
            state.values.append(value)

            if state.cooldown > 0:
                state.cooldown -= 1
                continue
            if len(window) < MIN_SAMPLES:
                continue

            corroborating = metric in CORROBORATING_ONLY
            ceiling = WATCHED.get(metric)
            baseline = _median(window)
            sigma = robust_z(window, value)

            anomaly: Anomaly | None = None

            # 1. Hard threshold breach — unambiguous, highest confidence.
            # Applies even to corroborating metrics: CPU actually pegged at
            # its ceiling is a fact, not an inference.
            if ceiling is not None and value >= ceiling:
                anomaly = Anomaly(
                    service=m.service, metric=metric, value=value, baseline=baseline,
                    deviation_sigma=sigma, severity=Severity.CRITICAL,
                    score=0.95, detector="threshold",
                    description=(f"{metric} at {value:.2f} has crossed its "
                                 f"threshold of {ceiling:.2f}"),
                    window=window[-20:],
                )

            # 2. Predicted breach — the pre-emptive path.
            elif (ceiling is not None and not corroborating
                  and len(window) >= MIN_SAMPLES_TREND):
                slope = linear_slope(window[-15:])
                if slope > 1e-6 and value < ceiling:
                    ticks = (ceiling - value) / slope
                    # Only interesting if it breaches soon but not instantly
                    # (an instant breach is noise, not a trend).
                    if 2 < ticks < 90:
                        seconds = ticks * 2.0  # fleet ticks every 2s
                        urgency = 1.0 - (ticks / 90.0)
                        anomaly = Anomaly(
                            service=m.service, metric=metric, value=value,
                            baseline=baseline, deviation_sigma=sigma,
                            severity=(Severity.CRITICAL if ticks < 30
                                      else Severity.WARNING),
                            score=round(0.55 + 0.4 * urgency, 3),
                            detector="trend_forecast",
                            predicted_breach_seconds=round(seconds, 1),
                            description=(
                                f"{metric} rising at {slope:.3f}/sample; projected to "
                                f"cross {ceiling:.2f} in ~{seconds:.0f}s"
                            ),
                            window=window[-20:],
                        )

            # 3. Level shift.
            if anomaly is None and sigma >= 4.0 and not corroborating:
                anomaly = Anomaly(
                    service=m.service, metric=metric, value=value, baseline=baseline,
                    deviation_sigma=round(sigma, 2),
                    severity=Severity.WARNING if sigma < 8 else Severity.CRITICAL,
                    score=round(min(0.9, 0.35 + sigma / 20.0), 3),
                    detector="zscore",
                    description=(f"{metric} at {value:.2f} is {sigma:.1f}σ from its "
                                 f"baseline of {baseline:.2f}"),
                    window=window[-20:],
                )

            if anomaly is not None:
                state.cooldown = 10
                found.append(anomaly)

        # 4. Multivariate pass.
        feature_vec = [values[k] for k in sorted(values)]
        forest_score = self._forest.observe(m.service, feature_vec)
        if forest_score is not None and forest_score > 0.62:
            key = (m.service, "__multivariate__")
            state = self._series[key]
            if state.cooldown == 0:
                state.cooldown = 12
                found.append(Anomaly(
                    service=m.service, metric="multivariate",
                    value=round(forest_score, 3), baseline=0.0,
                    deviation_sigma=0.0,
                    severity=Severity.WARNING if forest_score < 0.8 else Severity.CRITICAL,
                    score=round(forest_score, 3), detector="isolation_forest",
                    description=(
                        f"{m.service} metric combination is jointly anomalous "
                        f"(score {forest_score:.2f}) even though individual "
                        f"metrics may look normal"
                    ),
                ))
            else:
                state.cooldown -= 1

        return found


def correlate(anomalies: Iterable[Anomaly]) -> dict[str, list[Anomaly]]:
    """Group anomalies by service — the crudest useful correlation."""
    out: dict[str, list[Anomaly]] = defaultdict(list)
    for a in anomalies:
        out[a.service].append(a)
    return dict(out)
