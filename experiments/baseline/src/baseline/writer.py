"""SQL stand writer: ``transactions.raw`` into ``baseline.transactions``."""

import asyncio
import json
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from psycopg import AsyncConnection, InterfaceError, OperationalError
from psycopg.rows import TupleRow
from pydantic import ValidationError

from baseline.metrics import WRITER_METRICS, WriterMetrics, start_metrics_server
from baseline.settings import Settings
from schemas import TransactionEvent

_INSERT = """
INSERT INTO baseline.transactions (
    event_id, card_token, merchant_id, amount_minor, status,
    event_time, ingest_time, lat, lon
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
"""

_LOG_FIELDS = ("event_id", "partition", "offset", "outcome")
Sleep = Callable[[float], Awaitable[None]]
Row = tuple[object, ...]


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
    logger = logging.getLogger("baseline")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def wall_now() -> datetime:
    return datetime.now(UTC)


def _event_id_from_bytes(data: bytes) -> str | None:
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


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (OperationalError, InterfaceError, TimeoutError, OSError, ConnectionError)):
        return True
    return type(exc).__module__.startswith("aiokafka")


def _exc_info(exc: Exception) -> tuple[type[Exception], Exception, Any]:
    return (type(exc), exc, exc.__traceback__)


async def _quiet(call: Callable[[], Awaitable[object]]) -> None:
    try:
        await call()
    except Exception:
        logging.getLogger("baseline").warning("shutdown step failed", exc_info=True)


async def _retry(
    action: Callable[[], Awaitable[object]],
    message: str,
    settings: Settings,
    sleep: Sleep,
) -> None:
    backoff = settings.connect_backoff_s
    while True:
        try:
            await action()
            return
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            logging.getLogger("baseline").warning(message, exc_info=_exc_info(exc))
            await sleep(backoff)
            backoff = min(backoff * 2, settings.connect_backoff_max_s)


def _row(event: TransactionEvent) -> Row:
    return (
        event.event_id,
        event.card_token,
        event.merchant_id,
        event.amount_minor,
        event.status,
        event.event_time,
        event.ingest_time,
        event.lat,
        event.lon,
    )


class BaselineWriter:
    """Consume ``transactions.raw`` and insert rows into Postgres."""

    def __init__(
        self,
        settings: Settings,
        consumer: AIOKafkaConsumer,
        conn: AsyncConnection[TupleRow],
        *,
        now: Callable[[], datetime] = wall_now,
        sleep: Sleep = asyncio.sleep,
        metrics: WriterMetrics | None = None,
    ) -> None:
        self.settings = settings
        self.consumer = consumer
        self.conn = conn
        self.now = now
        self.sleep = sleep
        self.metrics = WRITER_METRICS if metrics is None else metrics
        self._closed = False

    @classmethod
    async def connect(
        cls,
        settings: Settings,
        *,
        now: Callable[[], datetime] = wall_now,
        sleep: Sleep = asyncio.sleep,
        metrics: WriterMetrics | None = None,
    ) -> "BaselineWriter":
        """Connect Kafka and Postgres and return a writer that owns them."""
        consumer = AIOKafkaConsumer(
            settings.raw_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=settings.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id="baseline-writer",
        )
        await _retry(consumer.start, "kafka connection failed", settings, sleep)
        try:
            conn = await _open_db(settings, sleep)
        except Exception:
            await _quiet(consumer.stop)
            raise
        return cls(settings, consumer, conn, now=now, sleep=sleep, metrics=metrics)

    async def aclose(self) -> None:
        """Stop the consumer and close Postgres. A second call does nothing."""
        if self._closed:
            return
        self._closed = True
        await _quiet(self.consumer.stop)
        await _quiet(self.conn.close)

    async def consume(self) -> None:
        """Insert each fetched batch, then commit offsets past it."""
        while True:
            batches = await self.consumer.getmany(
                timeout_ms=self.settings.batch_wait_ms,
                max_records=self.settings.batch_max_records,
            )
            await self._refresh_lag()
            records = [record for batch in batches.values() for record in batch]
            if records:
                await self._handle(records)

    async def _handle(self, records: list[ConsumerRecord]) -> None:
        valid: list[TransactionEvent] = []
        for record in records:
            event = self._parse(record)
            if event is not None:
                valid.append(event)
        if valid:
            await self._insert([_row(event) for event in valid])
        await self._commit(records)
        if not valid:
            return
        committed_at = self.now()
        for event in valid:
            elapsed = (committed_at - event.ingest_time).total_seconds()
            self.metrics.event_to_store.observe(max(0.0, elapsed))

    def _parse(self, record: ConsumerRecord) -> TransactionEvent | None:
        payload = record.value
        if not isinstance(payload, bytes):
            self._skip(record, None)
            return None
        try:
            return TransactionEvent.from_bytes(payload)
        except ValidationError:
            self._skip(record, _event_id_from_bytes(payload))
            return None

    async def _insert(self, rows: list[Row]) -> None:
        backoff = self.settings.connect_backoff_s
        while True:
            try:
                async with self.conn.transaction():
                    async with self.conn.cursor() as cur:
                        await cur.executemany(_INSERT, rows)
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                self._log_failure("insert failed", exc)
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.connect_backoff_max_s)
                try:
                    await self._reconnect()
                except Exception as reconnect_exc:
                    if not _is_retryable(reconnect_exc):
                        raise
                    self._log_failure("postgres reconnect failed", reconnect_exc)

    async def _reconnect(self) -> None:
        fresh = await AsyncConnection.connect(
            self.settings.database_url,
            prepare_threshold=0,
            connect_timeout=5,
        )
        old = self.conn
        self.conn = fresh
        await _quiet(old.close)

    async def _commit(self, records: list[ConsumerRecord]) -> None:
        offsets: dict[TopicPartition, int] = {}
        for record in records:
            tp = TopicPartition(record.topic, record.partition)
            nxt = record.offset + 1
            current = offsets.get(tp)
            if current is None or nxt > current:
                offsets[tp] = nxt
        backoff = self.settings.connect_backoff_s
        while True:
            try:
                await self.consumer.commit(offsets)
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                self._log_failure("commit failed", exc)
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.connect_backoff_max_s)

    async def _refresh_lag(self) -> None:
        assignment = [
            tp for tp in self.consumer.assignment() if tp.topic == self.settings.raw_topic
        ]
        if not assignment:
            return
        try:
            ends = await self.consumer.end_offsets(assignment)
            for tp in assignment:
                position = await self.consumer.position(tp)
                self.metrics.consumer_lag.labels(partition=str(tp.partition)).set(
                    ends[tp] - position
                )
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            logging.getLogger("baseline").warning("lag refresh failed", exc_info=_exc_info(exc))

    def _skip(self, record: ConsumerRecord, event_id: str | None) -> None:
        logging.getLogger("baseline").info(
            "skipped invalid record",
            extra={
                "event_id": event_id,
                "partition": record.partition,
                "offset": record.offset,
                "outcome": "skipped",
            },
        )

    def _log_failure(self, message: str, exc: Exception) -> None:
        logging.getLogger("baseline").warning(message, exc_info=_exc_info(exc))


async def _open_db(settings: Settings, sleep: Sleep) -> AsyncConnection[TupleRow]:
    holder: list[AsyncConnection[TupleRow]] = []

    async def open_conn() -> None:
        holder.append(
            await AsyncConnection.connect(
                settings.database_url,
                prepare_threshold=0,
                connect_timeout=5,
            )
        )

    await _retry(open_conn, "postgres connection failed", settings, sleep)
    return holder[0]


async def serve(
    settings: Settings,
    *,
    metrics: WriterMetrics | None = None,
    now: Callable[[], datetime] | None = None,
) -> None:
    """Connect and consume until the task is cancelled."""
    configure_logging()
    writer = await BaselineWriter.connect(
        settings,
        metrics=metrics,
        now=wall_now if now is None else now,
    )
    try:
        await writer.consume()
    finally:
        await writer.aclose()


async def run(settings: Settings) -> None:
    """Run until SIGINT or SIGTERM."""
    start_metrics_server(settings.metrics_port, WRITER_METRICS.registry)
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(serve(settings), name="baseline-writer")
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
