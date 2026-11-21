"""Offline writer: Kafka topics into ClickHouse (ADR-001, ADR-004, ADR-018)."""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import clickhouse_connect
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from pydantic import ValidationError

from features.metrics import OFFLINE_METRICS, OfflineMetrics, start_metrics_server
from features.settings import Settings
from schemas import FeatureSnapshot, TransactionEvent

_OFFLINE_GROUP = "offline-writer"
_EVENT_COLUMNS = (
    "schema_version",
    "event_id",
    "payment_id",
    "merchant_id",
    "amount_minor",
    "currency",
    "status",
    "event_time",
    "card_token",
    "lat",
    "lon",
    "ingest_time",
    "kafka_partition",
    "kafka_offset",
)
_SNAPSHOT_COLUMNS = (
    "entity_type",
    "entity_id",
    "event_id",
    "event_time",
    "ingest_time",
    "features",
)
_LOG_FIELDS = ("event_id", "partition", "offset", "outcome", "topic", "skipped")
Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], float]
WallClock = Callable[[], datetime]
EventRow = tuple[object, ...]
SnapshotRow = tuple[object, ...]


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with the writer fields when present."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in _LOG_FIELDS:
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    """Attach the JSON formatter once."""
    logger = logging.getLogger("features.offline")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def wall_now() -> datetime:
    return datetime.now(UTC)


def event_id_from_bytes(data: bytes) -> str | None:
    """Best-effort ``event_id`` from a payload that failed validation."""
    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if isinstance(payload, dict):
        event_id = payload.get("event_id")
        if isinstance(event_id, str):
            return event_id
    return None


def event_row(event: TransactionEvent, partition: int, offset: int) -> EventRow:
    """Map a transaction event to a ClickHouse ``events`` row (no ``written_at``)."""
    return (
        event.schema_version,
        event.event_id,
        event.payment_id,
        event.merchant_id,
        event.amount_minor,
        event.currency,
        event.status,
        event.event_time,
        event.card_token,
        event.lat,
        event.lon,
        event.ingest_time,
        partition,
        offset,
    )


def snapshot_row(snapshot: FeatureSnapshot) -> SnapshotRow:
    """Map a feature snapshot to a ClickHouse ``feature_snapshots`` row."""
    features = {name: float(value) for name, value in snapshot.features.items()}
    return (
        snapshot.entity_type,
        snapshot.entity_id,
        snapshot.event_id,
        snapshot.event_time,
        snapshot.ingest_time,
        features,
    )


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (TimeoutError, OSError, ConnectionError)):
        return True
    module = type(exc).__module__
    return module.startswith("aiokafka") or module.startswith("clickhouse_connect")


def _exc_info(exc: Exception) -> tuple[type[Exception], Exception, Any]:
    return (type(exc), exc, exc.__traceback__)


async def _quiet(call: Callable[[], Awaitable[object]]) -> None:
    try:
        await call()
    except Exception:
        logging.getLogger("features.offline").warning("shutdown step failed", exc_info=True)


@dataclass
class _Buffered:
    record: ConsumerRecord
    event_row: EventRow | None = None
    snapshot_row: SnapshotRow | None = None
    ingest_time: datetime | None = None


@dataclass
class OfflineWriter:
    """Consume both topics and batch-insert into ClickHouse."""

    settings: Settings
    consumer: AIOKafkaConsumer
    clickhouse: Any
    now: WallClock = wall_now
    monotonic: Clock = time.monotonic
    sleep: Sleep = asyncio.sleep
    metrics: OfflineMetrics = field(default_factory=lambda: OFFLINE_METRICS)
    _buffer: list[_Buffered] = field(default_factory=list)
    _first_buffered_at: float | None = None
    _closed: bool = False
    _flush_backoff_s: float | None = None

    @classmethod
    async def connect(
        cls,
        settings: Settings,
        *,
        now: WallClock = wall_now,
        monotonic: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        metrics: OfflineMetrics | None = None,
    ) -> OfflineWriter:
        """Build Kafka and ClickHouse clients and return a writer that owns them."""
        consumer = AIOKafkaConsumer(
            settings.raw_topic,
            settings.updates_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=_OFFLINE_GROUP,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id="features-offline-writer",
        )
        await cls._retry_static(
            consumer.start,
            "kafka consumer connection failed",
            settings,
            sleep,
        )
        try:
            client = await cls._open_clickhouse(settings)
        except Exception:
            await _quiet(consumer.stop)
            raise
        return cls(
            settings,
            consumer,
            client,
            now=now,
            monotonic=monotonic,
            sleep=sleep,
            metrics=OFFLINE_METRICS if metrics is None else metrics,
        )

    @staticmethod
    async def _open_clickhouse(settings: Settings) -> Any:
        return await clickhouse_connect.get_async_client(
            dsn=settings.clickhouse_url,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
        )

    async def aclose(self) -> None:
        """Stop Kafka and close ClickHouse. A second call does nothing."""
        if self._closed:
            return
        self._closed = True
        await _quiet(self.consumer.stop)
        await _quiet(self.clickhouse.close)

    async def run(self) -> None:
        """Fetch, buffer, flush, and commit until cancelled."""
        while True:
            if self._buffer and self._flush_backoff_s is not None:
                await self._flush_with_retry()
                continue
            if self._should_flush():
                await self._flush_with_retry()
                continue
            timeout_ms = self._poll_timeout_ms()
            batches = await self.consumer.getmany(
                timeout_ms=timeout_ms,
                max_records=self.settings.offline_batch_rows,
            )
            await self._refresh_lag()
            if not batches:
                if self._buffer and self._should_flush():
                    await self._flush_with_retry()
                continue
            self._ingest(batches)
            if self._should_flush():
                await self._flush_with_retry()

    def _poll_timeout_ms(self) -> int:
        if self._first_buffered_at is None:
            return max(1, int(self.settings.offline_flush_interval_s * 1000))
        remaining = self.settings.offline_flush_interval_s - (
            self.monotonic() - self._first_buffered_at
        )
        if remaining <= 0:
            return 1
        return max(1, int(remaining * 1000))

    def _buffered_rows(self) -> int:
        return sum(
            1
            for item in self._buffer
            if item.event_row is not None or item.snapshot_row is not None
        )

    def _should_flush(self) -> bool:
        if not self._buffer:
            return False
        if self._buffered_rows() >= self.settings.offline_batch_rows:
            return True
        if self._first_buffered_at is None:
            return False
        elapsed = self.monotonic() - self._first_buffered_at
        return elapsed >= self.settings.offline_flush_interval_s

    def _ingest(self, batches: dict[TopicPartition, list[ConsumerRecord]]) -> None:
        for records in batches.values():
            for record in records:
                self._accept(record)

    def _accept(self, record: ConsumerRecord) -> None:
        if self._first_buffered_at is None:
            self._first_buffered_at = self.monotonic()
        payload = record.value
        if not isinstance(payload, bytes):
            self._skip(record, None)
            self._buffer.append(_Buffered(record=record))
            return
        if record.topic == self.settings.raw_topic:
            try:
                event = TransactionEvent.from_bytes(payload)
            except ValidationError:
                self._skip(record, event_id_from_bytes(payload))
                self._buffer.append(_Buffered(record=record))
                return
            self._buffer.append(
                _Buffered(
                    record=record,
                    event_row=event_row(event, record.partition, record.offset),
                    ingest_time=event.ingest_time,
                )
            )
            self.metrics.records.labels(outcome="buffered").inc()
            return
        try:
            snapshot = FeatureSnapshot.from_bytes(payload)
        except ValidationError:
            self._skip(record, event_id_from_bytes(payload))
            self._buffer.append(_Buffered(record=record))
            return
        self._buffer.append(
            _Buffered(
                record=record,
                snapshot_row=snapshot_row(snapshot),
                ingest_time=snapshot.ingest_time,
            )
        )
        self.metrics.records.labels(outcome="buffered").inc()

    async def _flush_with_retry(self) -> None:
        backoff = (
            self.settings.offline_backoff_s
            if self._flush_backoff_s is None
            else self._flush_backoff_s
        )
        while True:
            try:
                await self._flush()
                self._flush_backoff_s = None
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                self.metrics.flush_failures.inc()
                self._flush_backoff_s = min(backoff * 2, self.settings.offline_backoff_max_s)
                logging.getLogger("features.offline").warning(
                    "flush failed",
                    exc_info=_exc_info(exc),
                )
                await self._refresh_lag()
                await self._reconnect_clickhouse()
                await self.sleep(backoff)
                backoff = self._flush_backoff_s

    async def _reconnect_clickhouse(self) -> None:
        """Replace a dead ClickHouse client after stop/restart (ADR-018)."""
        old = self.clickhouse
        try:
            self.clickhouse = await self._open_clickhouse(self.settings)
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            logging.getLogger("features.offline").warning(
                "clickhouse reconnect failed",
                exc_info=_exc_info(exc),
            )
            return
        await _quiet(old.close)

    async def _flush(self) -> None:
        if not self._buffer:
            return
        events = [item.event_row for item in self._buffer if item.event_row is not None]
        snapshots = [item.snapshot_row for item in self._buffer if item.snapshot_row is not None]
        started = self.monotonic()
        if events:
            await self.clickhouse.insert(
                table="events",
                data=events,
                column_names=_EVENT_COLUMNS,
                database=self.settings.clickhouse_db,
            )
            self.metrics.rows_written.labels(table="events").inc(len(events))
        if snapshots:
            await self.clickhouse.insert(
                table="feature_snapshots",
                data=snapshots,
                column_names=_SNAPSHOT_COLUMNS,
                database=self.settings.clickhouse_db,
            )
            self.metrics.rows_written.labels(table="feature_snapshots").inc(len(snapshots))
        self.metrics.flush_duration.observe(self.monotonic() - started)
        flushed_at = self.now()
        for item in self._buffer:
            if item.ingest_time is None:
                continue
            elapsed = (flushed_at - item.ingest_time).total_seconds()
            self.metrics.event_to_store.observe(max(0.0, elapsed))
        await self._commit(self._buffer)
        self._buffer.clear()
        self._first_buffered_at = None

    async def _commit(self, items: list[_Buffered]) -> None:
        offsets: dict[TopicPartition, int] = {}
        for item in items:
            record = item.record
            tp = TopicPartition(record.topic, record.partition)
            nxt = record.offset + 1
            current = offsets.get(tp)
            if current is None or nxt > current:
                offsets[tp] = nxt
        if not offsets:
            return
        backoff = self.settings.offline_backoff_s
        while True:
            try:
                await self.consumer.commit(offsets)
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                logging.getLogger("features.offline").warning(
                    "commit failed",
                    exc_info=_exc_info(exc),
                )
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.offline_backoff_max_s)

    async def _refresh_lag(self) -> None:
        assignment = list(self.consumer.assignment())
        if not assignment:
            return
        try:
            ends = await self.consumer.end_offsets(assignment)
            for tp in assignment:
                # Use the committed offset so lag stays visible while a fetched
                # batch is held during ClickHouse retries (position already moved).
                committed = await self.consumer.committed(tp)
                if committed is None:
                    cursor = await self.consumer.position(tp)
                else:
                    cursor = committed
                self.metrics.consumer_lag.labels(
                    topic=tp.topic,
                    partition=str(tp.partition),
                ).set(max(0, ends[tp] - cursor))
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            logging.getLogger("features.offline").warning(
                "lag refresh failed",
                exc_info=_exc_info(exc),
            )

    def _skip(self, record: ConsumerRecord, event_id: str | None) -> None:
        self.metrics.records.labels(outcome="skipped").inc()
        logging.getLogger("features.offline").info(
            "skipped invalid record",
            extra={
                "event_id": event_id,
                "partition": record.partition,
                "offset": record.offset,
                "outcome": "skipped",
                "topic": record.topic,
            },
        )

    @staticmethod
    async def _retry_static(
        action: Callable[[], Awaitable[object]],
        message: str,
        settings: Settings,
        sleep: Sleep,
    ) -> None:
        backoff = settings.offline_backoff_s
        while True:
            try:
                await action()
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                logging.getLogger("features.offline").warning(message, exc_info=_exc_info(exc))
                await sleep(backoff)
                backoff = min(backoff * 2, settings.offline_backoff_max_s)


async def serve(settings: Settings) -> None:
    """Connect and run until the task is cancelled."""
    configure_logging()
    writer = await OfflineWriter.connect(settings)
    try:
        await writer.run()
    finally:
        await writer.aclose()


async def run(settings: Settings) -> None:
    """Run until SIGINT or SIGTERM."""
    start_metrics_server(settings.metrics_port, OFFLINE_METRICS.registry)
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(serve(settings), name="features-offline")
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        return
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)


def main() -> None:
    asyncio.run(run(Settings.from_env()))


if __name__ == "__main__":
    main()
