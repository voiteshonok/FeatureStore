"""System snapshot merge and change detection (ADR-022)."""

from __future__ import annotations

from datetime import UTC, datetime

from gateway.system.state import (
    LAG_THRESHOLD,
    ProbeBundle,
    ReadyProbe,
    WorkerProbe,
    build_snapshot,
    should_emit_change,
)
from schemas import ComponentState, SystemStreamEvent


def _at() -> datetime:
    return datetime(2026, 11, 21, 12, 0, 0, tzinfo=UTC)


def _healthy_bundle(*, prom_ok: bool = True, worker_lag: int | None = 0) -> ProbeBundle:
    now = _at()
    return ProbeBundle(
        checked_at=now,
        gateway_status="up",
        ready={
            "ingestion": ReadyProbe(
                service="ingestion",
                status="up",
                checked_at=now,
                deps={"kafka": "up", "postgres": "up"},
            ),
            "features-api": ReadyProbe(
                service="features-api",
                status="up",
                checked_at=now,
                deps={"redis": "up", "postgres": "up"},
            ),
            "detection": ReadyProbe(
                service="detection",
                status="up",
                checked_at=now,
                deps={"kafka": "up", "postgres": "up"},
            ),
        },
        workers={
            "features-worker": WorkerProbe(
                name="features-worker",
                status="up",
                checked_at=now,
                lag=worker_lag,
            ),
            "features-offline": WorkerProbe(
                name="features-offline",
                status="up",
                checked_at=now,
                lag=0 if prom_ok else None,
            ),
        },
        prom_ok=prom_ok,
    )


def _by_name(event: SystemStreamEvent) -> dict[str, ComponentState]:
    return {item.name: item for item in event.components}


def test_build_snapshot_normal_mode() -> None:
    event = build_snapshot(_healthy_bundle())
    assert event.mode == "normal"
    assert len(event.components) == 10
    assert _by_name(event)["Redis"].status == "up"
    assert _by_name(event)["ClickHouse"].status == "unknown"
    assert _by_name(event)["features-worker"].lag == 0


def test_redis_down_sets_degraded() -> None:
    bundle = _healthy_bundle()
    now = _at()
    ready = dict(bundle.ready)
    ready["features-api"] = ReadyProbe(
        service="features-api",
        status="down",
        checked_at=now,
        deps={"redis": "down", "postgres": "up"},
    )
    event = build_snapshot(
        ProbeBundle(
            checked_at=now,
            gateway_status="up",
            ready=ready,
            workers=bundle.workers,
            prom_ok=True,
        )
    )
    assert event.mode == "degraded"
    assert _by_name(event)["features-api"].status == "down"
    assert _by_name(event)["Redis"].status == "down"


def test_prometheus_down_keeps_statuses_and_null_lag() -> None:
    event = build_snapshot(_healthy_bundle(prom_ok=False, worker_lag=50))
    assert event.mode == "normal"
    worker = _by_name(event)["features-worker"]
    assert worker.status == "up"
    assert worker.lag is None
    assert _by_name(event)["features-offline"].lag is None


def test_should_emit_change_on_status_flip() -> None:
    before = build_snapshot(_healthy_bundle())
    now = _at()
    ready = dict(_healthy_bundle().ready)
    ready["features-api"] = ReadyProbe(
        service="features-api",
        status="down",
        checked_at=now,
        deps={"redis": "down", "postgres": "up"},
    )
    after = build_snapshot(
        ProbeBundle(
            checked_at=now,
            gateway_status="up",
            ready=ready,
            workers=_healthy_bundle().workers,
            prom_ok=True,
        )
    )
    assert should_emit_change(before, after) is True
    assert should_emit_change(after, after) is False


def test_should_emit_change_when_lag_crosses_threshold() -> None:
    low = build_snapshot(_healthy_bundle(worker_lag=LAG_THRESHOLD - 1))
    high = build_snapshot(_healthy_bundle(worker_lag=LAG_THRESHOLD))
    higher = build_snapshot(_healthy_bundle(worker_lag=LAG_THRESHOLD + 50))
    assert should_emit_change(low, high) is True
    assert should_emit_change(high, higher) is False
    assert should_emit_change(high, low) is True


def test_first_snapshot_always_emits() -> None:
    current = build_snapshot(_healthy_bundle())
    assert should_emit_change(None, current) is True
