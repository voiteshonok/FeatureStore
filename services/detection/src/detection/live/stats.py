"""Sliding 60 s stats window from feature snapshots (ADR-021)."""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import ceil
from threading import Lock

from schemas import FeatureSnapshot, StatsStreamEvent

Clock = Callable[[], datetime]
WINDOW_S = 60
# Fixed latency buckets in milliseconds for percentile estimates.
_LATENCY_EDGES_MS = (
    1,
    5,
    10,
    25,
    50,
    100,
    250,
    500,
    1_000,
    2_000,
    5_000,
    10_000,
    30_000,
    60_000,
)


@dataclass
class _SecondBucket:
    second: int
    events: int = 0
    alerts_by_rule: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    latency_counts: list[int] = field(default_factory=lambda: [0] * len(_LATENCY_EDGES_MS))
    cards: set[str] = field(default_factory=set)


class LiveStats:
    """Per-second buckets; ``record`` is non-blocking for the Kafka loop."""

    def __init__(self, *, clock: Clock | None = None) -> None:
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))
        self._lock = Lock()
        self._buckets: deque[_SecondBucket] = deque()

    def record(self, snapshot: FeatureSnapshot) -> None:
        """Count one applied snapshot. Safe to call from the consumer thread."""
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        second = int(now.timestamp())
        latency_ms = max(0.0, (now - snapshot.ingest_time).total_seconds() * 1000.0)
        with self._lock:
            bucket = self._bucket(second)
            bucket.events += 1
            if snapshot.entity_type == "card":
                bucket.cards.add(snapshot.entity_id)
            idx = _latency_index(latency_ms)
            bucket.latency_counts[idx] += 1
            self._trim(second)

    def record_alert(self, rule_name: str) -> None:
        """Count an alert fired in the current second (from the consumer)."""
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        second = int(now.timestamp())
        with self._lock:
            bucket = self._bucket(second)
            bucket.alerts_by_rule[rule_name] = bucket.alerts_by_rule.get(rule_name, 0) + 1
            self._trim(second)

    def snapshot(self) -> StatsStreamEvent:
        """Aggregate the last 60 s. Empty window yields zeros and null percentiles."""
        now = self._clock()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        second = int(now.timestamp())
        with self._lock:
            self._trim(second)
            # Last completed second for eps_1s (current second is still filling).
            last = second - 1
            events_1s = 0.0
            total_events = 0
            alerts: dict[str, int] = defaultdict(int)
            latency = [0] * len(_LATENCY_EDGES_MS)
            cards: set[str] = set()
            for bucket in self._buckets:
                if bucket.second == last:
                    events_1s = float(bucket.events)
                if second - WINDOW_S < bucket.second <= second:
                    total_events += bucket.events
                    for rule, count in bucket.alerts_by_rule.items():
                        alerts[rule] += count
                    for i, count in enumerate(bucket.latency_counts):
                        latency[i] += count
                    cards |= bucket.cards
            avg = total_events / float(WINDOW_S)
            p50 = _percentile(latency, 0.50)
            p95 = _percentile(latency, 0.95)
            return StatsStreamEvent(
                events_per_s=events_1s,
                events_per_s_avg_60s=avg,
                alerts_60s_by_rule=dict(alerts),
                latency_p50_ms=p50,
                latency_p95_ms=p95,
                active_cards_60s=len(cards),
            )

    def _bucket(self, second: int) -> _SecondBucket:
        if self._buckets and self._buckets[-1].second == second:
            return self._buckets[-1]
        bucket = _SecondBucket(second=second)
        self._buckets.append(bucket)
        return bucket

    def _trim(self, now_second: int) -> None:
        cutoff = now_second - WINDOW_S
        while self._buckets and self._buckets[0].second <= cutoff:
            self._buckets.popleft()


def _latency_index(latency_ms: float) -> int:
    for i, edge in enumerate(_LATENCY_EDGES_MS):
        if latency_ms <= edge:
            return i
    return len(_LATENCY_EDGES_MS) - 1


def _percentile(counts: list[int], q: float) -> float | None:
    total = sum(counts)
    if total == 0:
        return None
    target = max(1, ceil(total * q))
    seen = 0
    for edge, count in zip(_LATENCY_EDGES_MS, counts, strict=True):
        seen += count
        if seen >= target:
            return float(edge)
    return float(_LATENCY_EDGES_MS[-1])
