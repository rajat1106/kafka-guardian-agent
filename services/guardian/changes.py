"""Recent-change tracking, for answering "what changed?".

Keeps a short rolling window of change events in memory and matches them to
an incident by service and time. The window is deliberately small: a deploy
six hours before an incident is not evidence, and offering it as evidence
teaches operators to distrust the correlation.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Deque

import structlog

from guardian_platform.contracts import ChangeEvent

log = structlog.get_logger(__name__)

# How far back a change can be and still plausibly explain an incident.
# Long enough for a slow rollout to bite, short enough to stay meaningful.
CORRELATION_WINDOW = timedelta(minutes=30)
MAX_TRACKED = 500


class ChangeTracker:
    def __init__(self) -> None:
        self._events: Deque[ChangeEvent] = deque(maxlen=MAX_TRACKED)

    def record(self, event: ChangeEvent) -> None:
        self._events.append(event)
        log.info("change_recorded", kind=event.kind.value, service=event.service,
                 summary=event.summary[:80], reference=event.reference)

    def correlate(
        self, service: str, at: datetime | None = None
    ) -> list[ChangeEvent]:
        """Changes to this service shortly before `at`, newest first.

        Also returns changes to *other* services in the window, ranked lower,
        because a deploy to an upstream dependency explains a downstream
        incident and the agent has no dependency graph to know that.
        """
        moment = at or datetime.now(timezone.utc)
        earliest = moment - CORRELATION_WINDOW

        same, others = [], []
        for e in self._events:
            ts = e.ts if e.ts.tzinfo else e.ts.replace(tzinfo=timezone.utc)
            if not (earliest <= ts <= moment + timedelta(seconds=30)):
                continue
            (same if e.service == service else others).append(e)

        same.sort(key=lambda e: e.ts, reverse=True)
        others.sort(key=lambda e: e.ts, reverse=True)
        return same + others[:3]

    def describe(self, service: str, at: datetime | None = None) -> list[str]:
        """Human-readable correlation lines for the prompt and the UI."""
        moment = at or datetime.now(timezone.utc)
        out = []
        for e in self.correlate(service, moment):
            ts = e.ts if e.ts.tzinfo else e.ts.replace(tzinfo=timezone.utc)
            delta = (moment - ts).total_seconds()
            when = (f"{delta:.0f}s before" if delta >= 0
                    else f"{-delta:.0f}s after")
            scope = "same service" if e.service == service else f"on {e.service}"
            ref = f" [{e.reference}]" if e.reference else ""
            out.append(f"{e.kind.value} {when} onset, {scope}: {e.summary}{ref}")
        return out

    @property
    def count(self) -> int:
        return len(self._events)

    def recent(self, limit: int = 20) -> list[ChangeEvent]:
        return list(self._events)[-limit:][::-1]
