"""Small statistical helpers shared across services.

These live in the shared package rather than inside the detector because
more than one service needs them: the detector fits slopes to spot trends,
and the planner fits a slope to the same window to size a remedy against
the observed deficit. Importing across service directories only works when
both happen to be on the path, which is a property of how a process was
launched rather than anything the code guarantees.
"""

from __future__ import annotations


def median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2


def mad(xs: list[float]) -> float:
    """Median absolute deviation."""
    med = median(xs)
    return median([abs(x - med) for x in xs])


def robust_z(window: list[float], value: float) -> float:
    """Median/MAD z-score.

    Preferred over mean/stdev because a single large spike inflates the
    standard deviation and masks the very anomaly that produced it.
    """
    med = median(window)
    dispersion = mad(window)
    if dispersion < 1e-9:
        # A perfectly flat series: fall back to a relative-change test so a
        # jump from 0 to 500 is not silently scored as zero deviation.
        if abs(med) < 1e-9:
            return 0.0 if abs(value) < 1e-9 else 6.0
        return abs(value - med) / abs(med) * 3.0
    # 0.6745 converts MAD to a stdev-equivalent scale for normal data.
    return abs(value - med) * 0.6745 / dispersion


def linear_slope(window: list[float]) -> float:
    """Least-squares slope in units per sample."""
    n = len(window)
    if n < 3:
        return 0.0
    mean_x = (n - 1) / 2
    mean_y = sum(window) / n
    num = sum((i - mean_x) * (y - mean_y) for i, y in enumerate(window))
    den = sum((i - mean_x) ** 2 for i in range(n))
    return num / den if den else 0.0
