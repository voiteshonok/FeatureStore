"""Sliding window stats with an injectable clock."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from detection.live.stats import LiveStats
from schemas import FeatureSnapshot


class _Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


def _snapshot(*, entity_id: str = "card-1", ingest_offset_s: float = 0.1) -> FeatureSnapshot:
    clock_now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    ingest = clock_now - timedelta(seconds=ingest_offset_s)
    return FeatureSnapshot(
        entity_type="card",
        entity_id=entity_id,
        event_id=uuid4(),
        event_time=ingest,
        ingest_time=ingest,
        features={"card_cnt_1m": 1},
        source_partition=0,
        source_offset=0,
    )


def test_empty_window() -> None:
    clock = _Clock(datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC))
    stats = LiveStats(clock=clock)
    snap = stats.snapshot()
    assert snap.events_per_s == 0.0
    assert snap.events_per_s_avg_60s == 0.0
    assert snap.active_cards_60s == 0
    assert snap.latency_p50_ms is None
    assert snap.latency_p95_ms is None


def test_buckets_evict_after_60s() -> None:
    clock = _Clock(datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC))
    stats = LiveStats(clock=clock)
    # Align snapshot ingest with clock for recording.
    snap = FeatureSnapshot(
        entity_type="card",
        entity_id="card-1",
        event_id=uuid4(),
        event_time=clock.now,
        ingest_time=clock.now - timedelta(milliseconds=50),
        features={"card_cnt_1m": 1},
        source_partition=0,
        source_offset=0,
    )
    stats.record(snap)
    clock.advance(61)
    out = stats.snapshot()
    assert out.events_per_s_avg_60s == 0.0
    assert out.active_cards_60s == 0


def test_counts_and_alerts() -> None:
    start = datetime(2026, 1, 1, 12, 0, 10, tzinfo=UTC)
    clock = _Clock(start)
    stats = LiveStats(clock=clock)
    for i in range(3):
        snap = FeatureSnapshot(
            entity_type="card",
            entity_id=f"card-{i % 2}",
            event_id=uuid4(),
            event_time=clock.now,
            ingest_time=clock.now - timedelta(milliseconds=100),
            features={"card_cnt_1m": 1},
            source_partition=0,
            source_offset=i,
        )
        stats.record(snap)
    stats.record_alert("card_velocity_burst")
    clock.advance(1)
    out = stats.snapshot()
    assert out.events_per_s == 3.0
    assert out.active_cards_60s == 2
    assert out.alerts_60s_by_rule.get("card_velocity_burst") == 1
    assert out.latency_p50_ms is not None
