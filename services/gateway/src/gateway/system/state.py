"""Pure merge of probe results into a system snapshot (ADR-022)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from schemas import ComponentState, SystemStreamEvent
from schemas.stream import ComponentName, ComponentStatus, SystemMode

LAG_THRESHOLD = 1000

COMPONENT_ORDER: tuple[ComponentName, ...] = (
    "ingestion",
    "features-worker",
    "features-offline",
    "features-api",
    "detection",
    "gateway",
    "Kafka",
    "Redis",
    "PostgreSQL",
    "ClickHouse",
)

DepKey = Literal["kafka", "redis", "postgres"]


@dataclass(frozen=True)
class ReadyProbe:
    """One HTTP ``/ready`` response (or failure)."""

    service: Literal["ingestion", "features-api", "detection"]
    status: ComponentStatus
    checked_at: datetime
    deps: dict[DepKey, ComponentStatus]


@dataclass(frozen=True)
class WorkerProbe:
    """Prometheus ``up`` + lag for a worker without HTTP."""

    name: Literal["features-worker", "features-offline"]
    status: ComponentStatus
    checked_at: datetime
    lag: int | None


@dataclass(frozen=True)
class ProbeBundle:
    """Inputs for one collector tick."""

    checked_at: datetime
    gateway_status: ComponentStatus
    ready: dict[str, ReadyProbe]
    workers: dict[str, WorkerProbe]
    prom_ok: bool


def _dep_status(
    ready: dict[str, ReadyProbe],
    key: DepKey,
    *services: str,
) -> ComponentStatus:
    seen = False
    any_down = False
    for name in services:
        probe = ready.get(name)
        if probe is None:
            continue
        value = probe.deps.get(key)
        if value is None:
            continue
        seen = True
        if value == "down":
            any_down = True
    if not seen:
        return "unknown"
    if any_down:
        return "down"
    return "up"


def _service_status(ready: dict[str, ReadyProbe], name: str) -> ComponentStatus:
    probe = ready.get(name)
    if probe is None:
        return "unknown"
    return probe.status


def _checked(ready: dict[str, ReadyProbe], name: str, fallback: datetime) -> datetime:
    probe = ready.get(name)
    if probe is None:
        return fallback
    return probe.checked_at


def mode_for(components: list[ComponentState]) -> SystemMode:
    if any(item.status == "down" for item in components):
        return "degraded"
    return "normal"


def build_snapshot(bundle: ProbeBundle) -> SystemStreamEvent:
    """Compose the full component list from the latest probes."""
    at = bundle.checked_at
    workers = bundle.workers
    ready = bundle.ready

    by_name: dict[ComponentName, ComponentState] = {
        "ingestion": ComponentState(
            name="ingestion",
            status=_service_status(ready, "ingestion"),
            checked_at=_checked(ready, "ingestion", at),
            lag=None,
        ),
        "features-api": ComponentState(
            name="features-api",
            status=_service_status(ready, "features-api"),
            checked_at=_checked(ready, "features-api", at),
            lag=None,
        ),
        "detection": ComponentState(
            name="detection",
            status=_service_status(ready, "detection"),
            checked_at=_checked(ready, "detection", at),
            lag=None,
        ),
        "gateway": ComponentState(
            name="gateway",
            status=bundle.gateway_status,
            checked_at=at,
            lag=None,
        ),
        "Kafka": ComponentState(
            name="Kafka",
            status=_dep_status(ready, "kafka", "detection", "ingestion"),
            checked_at=at,
            lag=None,
        ),
        "Redis": ComponentState(
            name="Redis",
            status=_dep_status(ready, "redis", "features-api"),
            checked_at=at,
            lag=None,
        ),
        "PostgreSQL": ComponentState(
            name="PostgreSQL",
            status=_dep_status(ready, "postgres", "detection", "ingestion", "features-api"),
            checked_at=at,
            lag=None,
        ),
        "ClickHouse": ComponentState(
            name="ClickHouse",
            status="unknown",
            checked_at=at,
            lag=None,
        ),
    }

    worker_names: tuple[Literal["features-worker", "features-offline"], ...] = (
        "features-worker",
        "features-offline",
    )
    for worker_name in worker_names:
        worker = workers.get(worker_name)
        if worker is None:
            by_name[worker_name] = ComponentState(
                name=worker_name,
                status="unknown",
                checked_at=at,
                lag=None,
            )
        else:
            by_name[worker_name] = ComponentState(
                name=worker.name,
                status=worker.status,
                checked_at=worker.checked_at,
                lag=worker.lag if bundle.prom_ok else None,
            )

    components = [by_name[name] for name in COMPONENT_ORDER]
    return SystemStreamEvent(mode=mode_for(components), components=components)


def _status_map(event: SystemStreamEvent) -> dict[str, tuple[ComponentStatus, int | None]]:
    return {item.name: (item.status, item.lag) for item in event.components}


def should_emit_change(
    previous: SystemStreamEvent | None,
    current: SystemStreamEvent,
    *,
    lag_threshold: int = LAG_THRESHOLD,
) -> bool:
    """True when status flipped or lag crossed the threshold either way."""
    if previous is None:
        return True
    if previous.mode != current.mode:
        return True
    before = _status_map(previous)
    after = _status_map(current)
    for name, (status, lag) in after.items():
        old_status, old_lag = before.get(name, ("unknown", None))
        if status != old_status:
            return True
        if _crossed_threshold(old_lag, lag, lag_threshold):
            return True
    return False


def _crossed_threshold(old: int | None, new: int | None, threshold: int) -> bool:
    if old is None and new is None:
        return False
    if old is None or new is None:
        # Appearing/disappearing lag is not a threshold cross by itself.
        return False
    was_high = old >= threshold
    is_high = new >= threshold
    return was_high != is_high
