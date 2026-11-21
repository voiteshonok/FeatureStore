"""Stream event schema round-trips (ADR-021)."""

from datetime import UTC, datetime
from uuid import uuid4

from schemas import AlertStreamEvent, ResetStreamEvent, StatsStreamEvent


def test_alert_stream_event_roundtrip() -> None:
    event = AlertStreamEvent(
        id=1,
        op="created",
        alert_id=uuid4(),
        rule_id=uuid4(),
        rule_name="card_velocity_burst",
        status="open",
        entity="card",
        entity_id="abcd…ef",
        created_at=datetime.now(UTC),
    )
    restored = AlertStreamEvent.model_validate_json(event.model_dump_json())
    assert restored == event


def test_stats_and_reset() -> None:
    stats = StatsStreamEvent(
        events_per_s=12.0,
        events_per_s_avg_60s=10.5,
        alerts_60s_by_rule={"card_velocity_burst": 2},
        latency_p50_ms=50.0,
        latency_p95_ms=250.0,
        active_cards_60s=7,
    )
    assert StatsStreamEvent.model_validate(stats.model_dump()) == stats
    assert ResetStreamEvent(reason="reconnect").reason == "reconnect"
