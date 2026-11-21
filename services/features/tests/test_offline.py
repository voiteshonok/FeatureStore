"""Unit tests for the offline writer (ADR-018)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

import pytest
from prometheus_client import CollectorRegistry
from support import BASE, make_event

from features.metrics import OfflineMetrics, offline_metrics
from features.offline import OfflineWriter, event_row, snapshot_row
from features.settings import Settings
from schemas import FeatureSnapshot

TOPIC_RAW = "transactions.raw"
TOPIC_UPDATES = "features.updates"
EVENT_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")


@dataclass(frozen=True)
class Partition:
    topic: str
    partition: int


class Record:
    def __init__(
        self,
        value: bytes | None,
        *,
        partition: int = 0,
        offset: int = 0,
        topic: str = TOPIC_RAW,
    ) -> None:
        self.value = value
        self.partition = partition
        self.offset = offset
        self.topic = topic


class FakeConsumer:
    def __init__(self) -> None:
        self.commits: list[dict[object, int]] = []
        self._batches: list[dict[Partition, list[Record]]] = []
        self._assignment: set[Partition] = set()
        self.positions: dict[Partition, int] = {}
        self.ends: dict[Partition, int] = {}

    def queue(self, batches: dict[Partition, list[Record]]) -> None:
        self._batches.append(batches)

    def assignment(self) -> set[Partition]:
        return self._assignment

    async def position(self, tp: Partition) -> int:
        return self.positions.get(tp, 0)

    async def end_offsets(self, partitions: list[Partition]) -> dict[Partition, int]:
        return {tp: self.ends.get(tp, 0) for tp in partitions}

    async def getmany(
        self, *, timeout_ms: int = 0, max_records: int = 0
    ) -> dict[Partition, list[Record]]:
        if self._batches:
            return self._batches.pop(0)
        return {}

    async def commit(self, offsets: dict[object, int]) -> None:
        self.commits.append(offsets)


class FakeClickHouse:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[object], object]] = []
        self.fail_times = 0

    async def insert(
        self,
        table: str | None = None,
        data: list[object] | None = None,
        column_names: object = "*",
        database: str | None = None,
        **kwargs: object,
    ) -> None:
        if self.fail_times:
            self.fail_times -= 1
            raise ConnectionError("clickhouse down")
        assert table is not None
        assert data is not None
        self.inserts.append((table, list(data), column_names))

    async def close(self) -> None:
        return None


@dataclass
class FakeClock:
    mono: float = 0.0
    wall: datetime = field(default_factory=lambda: BASE)

    def monotonic(self) -> float:
        return self.mono

    def now(self) -> datetime:
        return self.wall


async def _noop_sleep(_seconds: float) -> None:
    return None


def _settings(**overrides: object) -> Settings:
    payload: dict[str, object] = {
        "offline_batch_rows": 100,
        "offline_flush_interval_s": 1.0,
        "offline_backoff_s": 0.01,
        "offline_backoff_max_s": 0.05,
    }
    payload.update(overrides)
    return Settings(**payload)  # type: ignore[arg-type]


def _writer(
    consumer: FakeConsumer,
    clickhouse: FakeClickHouse,
    *,
    settings: Settings | None = None,
    clock: FakeClock | None = None,
    metrics: OfflineMetrics | None = None,
) -> OfflineWriter:
    clock = FakeClock() if clock is None else clock
    return OfflineWriter(
        settings or _settings(),
        consumer,  # type: ignore[arg-type]
        clickhouse,  # type: ignore[arg-type]
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=_noop_sleep,
        metrics=metrics or offline_metrics(CollectorRegistry()),
    )


def _snapshot(**overrides: object) -> FeatureSnapshot:
    payload: dict[str, object] = {
        "entity_type": "card",
        "entity_id": "card-1",
        "event_id": EVENT_ID,
        "event_time": BASE,
        "ingest_time": BASE,
        "features": {"card_cnt_1m": 1, "card_sum_1m": 100.5},
        "source_partition": 0,
        "source_offset": 0,
    }
    payload.update(overrides)
    return FeatureSnapshot.model_validate(payload)


def test_event_row_maps_all_fields_including_null_coords() -> None:
    event = make_event(event_id=EVENT_ID, lat=None, lon=None)
    row = event_row(event, partition=2, offset=9)
    assert row[0] == 1
    assert row[1] == EVENT_ID
    assert row[2] == event.payment_id
    assert row[3] == event.merchant_id
    assert row[4] == event.amount_minor
    assert row[5] == "EUR"
    assert row[6] == "approved"
    assert row[7] == event.event_time
    assert row[8] == event.card_token
    assert row[9] is None
    assert row[10] is None
    assert row[11] == event.ingest_time
    assert row[12] == 2
    assert row[13] == 9


def test_event_row_keeps_coordinates() -> None:
    event = make_event(event_id=EVENT_ID, lat=52.5, lon=13.4)
    row = event_row(event, 0, 1)
    assert row[9] == 52.5
    assert row[10] == 13.4


def test_snapshot_row_maps_features_as_float_map() -> None:
    snapshot = _snapshot(features={"card_cnt_1m": 2, "card_sum_1m": 50})
    row = snapshot_row(snapshot)
    assert row[0] == "card"
    assert row[1] == "card-1"
    assert row[2] == EVENT_ID
    assert row[5] == {"card_cnt_1m": 2.0, "card_sum_1m": 50.0}


@pytest.mark.asyncio
async def test_flush_by_batch_size() -> None:
    consumer = FakeConsumer()
    clickhouse = FakeClickHouse()
    writer = _writer(consumer, clickhouse, settings=_settings(offline_batch_rows=2))
    event_a = make_event(event_id=EVENT_ID, payment_id="pay-a")
    event_b = make_event(
        event_id=UUID("bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"),
        payment_id="pay-b",
    )
    consumer.queue(
        {
            Partition(TOPIC_RAW, 0): [
                Record(event_a.to_bytes(), offset=0),
                Record(event_b.to_bytes(), offset=1),
            ]
        }
    )
    batches = await consumer.getmany()
    writer._ingest(batches)  # noqa: SLF001
    assert writer._should_flush()  # noqa: SLF001
    await writer._flush_with_retry()  # noqa: SLF001
    assert len(clickhouse.inserts) == 1
    assert clickhouse.inserts[0][0] == "events"
    assert len(clickhouse.inserts[0][1]) == 2
    assert consumer.commits
    assert not writer._buffer  # noqa: SLF001


@pytest.mark.asyncio
async def test_flush_by_timer() -> None:
    consumer = FakeConsumer()
    clickhouse = FakeClickHouse()
    clock = FakeClock(mono=0.0)
    writer = _writer(
        consumer,
        clickhouse,
        settings=_settings(offline_batch_rows=100, offline_flush_interval_s=1.0),
        clock=clock,
    )
    event = make_event(event_id=EVENT_ID)
    consumer.queue({Partition(TOPIC_RAW, 0): [Record(event.to_bytes(), offset=0)]})
    writer._ingest(await consumer.getmany())  # noqa: SLF001
    assert not writer._should_flush()  # noqa: SLF001
    clock.mono = 1.0
    assert writer._should_flush()  # noqa: SLF001
    await writer._flush_with_retry()  # noqa: SLF001
    assert len(clickhouse.inserts) == 1
    assert consumer.commits


@pytest.mark.asyncio
async def test_insert_failure_keeps_batch_and_retries_same_rows() -> None:
    consumer = FakeConsumer()
    clickhouse = FakeClickHouse()
    clickhouse.fail_times = 1
    metrics = offline_metrics(CollectorRegistry())
    writer = _writer(consumer, clickhouse, metrics=metrics)
    event = make_event(event_id=EVENT_ID)
    snapshot = _snapshot()
    writer._ingest(  # noqa: SLF001
        {
            Partition(TOPIC_RAW, 0): [Record(event.to_bytes(), offset=0)],
            Partition(TOPIC_UPDATES, 0): [
                Record(snapshot.to_bytes(), offset=0, topic=TOPIC_UPDATES)
            ],
        }
    )
    first_rows = [item.event_row for item in writer._buffer if item.event_row]  # noqa: SLF001
    await writer._flush_with_retry()  # noqa: SLF001
    assert len(clickhouse.inserts) == 2
    assert clickhouse.inserts[0][0] == "events"
    assert clickhouse.inserts[0][1] == first_rows
    assert clickhouse.inserts[1][0] == "feature_snapshots"
    assert len(consumer.commits) == 1
    assert not writer._buffer  # noqa: SLF001


@pytest.mark.asyncio
async def test_commit_not_called_while_insert_keeps_failing() -> None:
    consumer = FakeConsumer()
    clickhouse = FakeClickHouse()
    clickhouse.fail_times = 100
    sleeps: list[float] = []

    async def track_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            clickhouse.fail_times = 0

    clock = FakeClock()
    writer = OfflineWriter(
        _settings(offline_batch_rows=1),
        consumer,  # type: ignore[arg-type]
        clickhouse,  # type: ignore[arg-type]
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=track_sleep,
        metrics=offline_metrics(CollectorRegistry()),
    )
    event = make_event(event_id=EVENT_ID)
    writer._ingest({Partition(TOPIC_RAW, 0): [Record(event.to_bytes(), offset=3)]})  # noqa: SLF001
    assert not consumer.commits
    await writer._flush_with_retry()  # noqa: SLF001
    assert len(sleeps) >= 2
    assert consumer.commits
    assert clickhouse.inserts


def test_offline_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLICKHOUSE_URL", "http://clickhouse:8123")
    monkeypatch.setenv("CLICKHOUSE_USER", "svc_offline")
    monkeypatch.setenv("SVC_OFFLINE_PASSWORD", "secret")
    monkeypatch.setenv("OFFLINE_BATCH_ROWS", "10")
    monkeypatch.setenv("OFFLINE_FLUSH_INTERVAL_S", "0.5")
    settings = Settings.from_env()
    assert settings.clickhouse_url == "http://clickhouse:8123"
    assert settings.clickhouse_user == "svc_offline"
    assert settings.clickhouse_password == "secret"  # noqa: S105
    assert settings.offline_batch_rows == 10
    assert settings.offline_flush_interval_s == 0.5


def test_offline_batch_rows_must_be_positive() -> None:
    with pytest.raises(ValueError, match="offline_batch_rows"):
        Settings(offline_batch_rows=0)
