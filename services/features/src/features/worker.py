"""Online writer: ``transactions.raw`` into Redis and ``features.updates``."""

import asyncio
import importlib.resources
import json
import logging
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from features.settings import Settings
from features.windows import (
    DAY_MS,
    MERCHANT_TTL_S,
    _await,
    card_key,
    card_stream_features,
    format_geo,
    geo_key,
    member,
    merchant_key,
    merchant_stream_features,
    minute_bucket,
    read_offset,
    to_epoch_ms,
    watermark_key,
)
from schemas import FeatureSnapshot, TransactionEvent
from schemas.catalog import Entity

_SCRIPT = importlib.resources.files("features").joinpath("apply.lua").read_text(encoding="utf-8")
_LOG_FIELDS = ("event_id", "partition", "offset", "outcome", "applied", "dup", "skipped")
Sleep = Callable[[float], Awaitable[None]]
Outcome = Literal["applied", "dup"]


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
                payload[key] = cast(object, getattr(record, key))
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    """Attach the JSON formatter once."""
    logger = logging.getLogger("features")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def wall_now_ms() -> int:
    return to_epoch_ms(datetime.now(UTC))


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


def _is_noscript(exc: Exception) -> bool:
    return isinstance(exc, ResponseError) and "NOSCRIPT" in str(exc)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, ResponseError):
        return False
    if isinstance(exc, (RedisError, TimeoutError, OSError)):
        return True
    return type(exc).__module__.startswith("aiokafka")


def _exc_info(exc: Exception) -> tuple[type[Exception], Exception, Any]:
    return (type(exc), exc, exc.__traceback__)


async def _quiet(call: Callable[[], Awaitable[object]]) -> None:
    try:
        await call()
    except Exception:
        logging.getLogger("features").warning("shutdown step failed", exc_info=True)


async def load_script(redis: Redis) -> str:
    """``SCRIPT LOAD`` the apply script and return its SHA."""
    sha = await _await(redis.script_load(_SCRIPT))
    return str(sha)


async def apply_event(
    redis: Redis,
    sha: str,
    event: TransactionEvent,
    *,
    partition: int,
    offset: int,
    now_ms: int,
) -> Outcome:
    """Apply one event. ``dup`` means the partition watermark already covers it."""
    event_time_ms = to_epoch_ms(event.event_time)
    lat = event.lat
    lon = event.lon
    if lat is not None and lon is not None:
        geo = format_geo(lat, lon, event_time_ms)
        has_geo = True
    else:
        geo = ""
        has_geo = False
    keys = [
        card_key(event.card_token),
        merchant_key(event.merchant_id, minute_bucket(event_time_ms)),
        watermark_key(partition),
        geo_key(event.card_token),
    ]
    args = [
        str(offset),
        str(event_time_ms),
        member(event.event_id, event.amount_minor),
        str(now_ms),
        str(DAY_MS),
        str(event.amount_minor),
        "1" if event.status == "declined" else "0",
        str(MERCHANT_TTL_S),
        str(to_epoch_ms(event.ingest_time)),
        "1" if has_geo else "0",
        geo,
    ]
    raw = await _await(redis.evalsha(sha, len(keys), *keys, *args))
    result = raw.decode() if isinstance(raw, bytes) else str(raw)
    if result == "applied":
        return "applied"
    if result == "dup":
        return "dup"
    msg = f"unexpected apply result {result!r}"
    raise RuntimeError(msg)


def _snapshot(
    entity_type: Entity,
    entity_id: str,
    event: TransactionEvent,
    features: dict[str, int | float],
    partition: int,
    offset: int,
) -> FeatureSnapshot:
    return FeatureSnapshot(
        entity_type=entity_type,
        entity_id=entity_id,
        event_id=event.event_id,
        event_time=event.event_time,
        ingest_time=event.ingest_time,
        features=features,
        source_partition=partition,
        source_offset=offset,
    )


async def publish_snapshots(
    producer: AIOKafkaProducer,
    redis: Redis,
    settings: Settings,
    event: TransactionEvent,
    *,
    partition: int,
    offset: int,
) -> None:
    """Publish the card snapshot and the merchant snapshot for one applied event."""
    t_ms = to_epoch_ms(event.event_time)
    card = _snapshot(
        "card",
        event.card_token,
        event,
        await card_stream_features(redis, event.card_token, t_ms),
        partition,
        offset,
    )
    merchant = _snapshot(
        "merchant",
        event.merchant_id,
        event,
        await merchant_stream_features(redis, event.merchant_id, t_ms),
        partition,
        offset,
    )
    for snapshot in (card, merchant):
        await producer.send_and_wait(
            settings.updates_topic,
            value=snapshot.to_bytes(),
            key=snapshot.entity_id.encode("utf-8"),
        )


@dataclass
class Counters:
    applied: int = 0
    dup: int = 0
    skipped: int = 0


class OnlineWriter:
    """Consume ``transactions.raw`` and maintain Redis windows."""

    def __init__(
        self,
        settings: Settings,
        redis: Redis,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        *,
        now_ms: Callable[[], int] = wall_now_ms,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.settings = settings
        self.redis = redis
        self.consumer = consumer
        self.producer = producer
        self.now_ms = now_ms
        self.sleep = sleep
        self.counters = Counters()
        self._sha: str | None = None
        self._closed = False

    @classmethod
    async def connect(cls, settings: Settings) -> "OnlineWriter":
        """Build clients, connect to Kafka, and return a writer that owns them."""
        redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
        )
        consumer = AIOKafkaConsumer(
            settings.raw_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=settings.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id="features-online-writer",
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap,
            acks="all",
            enable_idempotence=True,
            linger_ms=5,
            client_id="features-online-writer",
        )
        writer = cls(settings, redis, consumer, producer)
        await writer.start()
        return writer

    async def start(self) -> None:
        """Connect the consumer and the idempotent producer."""
        await self._retry(self.consumer.start, "kafka consumer connection failed")
        try:
            await self._retry(self.producer.start, "kafka producer connection failed")
        except Exception:
            await _quiet(self.consumer.stop)
            raise

    async def aclose(self) -> None:
        """Stop Kafka clients and close Redis. A second call does nothing."""
        if self._closed:
            return
        self._closed = True
        await _quiet(self.consumer.stop)
        await _quiet(self.producer.stop)
        await _quiet(self.redis.aclose)

    async def supervise(self) -> None:
        """Run the consume loop and the freshness heartbeat together."""
        async with asyncio.TaskGroup() as group:
            group.create_task(self.heartbeat(), name="features-heartbeat")
            group.create_task(self.consume(), name="features-consume")

    async def consume(self) -> None:
        """Load the script, then apply each record before reading the next."""
        await self._load_with_retry()
        async for record in self.consumer:
            await self.handle(record)

    async def handle(self, record: ConsumerRecord) -> None:
        """Apply one record, publish when the state changed, then commit."""
        payload = record.value
        if not isinstance(payload, bytes):
            self.counters.skipped += 1
            self._log(record, "skipped invalid record", outcome="skipped", event_id=None)
            await self._commit(record, None)
            return
        try:
            event = TransactionEvent.from_bytes(payload)
        except ValidationError:
            event_id = event_id_from_bytes(payload)
            self.counters.skipped += 1
            self._log(record, "skipped invalid record", outcome="skipped", event_id=event_id)
            await self._commit(record, event_id)
            return

        backoff = self.settings.redis_backoff_s
        seen: Outcome | None = None
        logged = False
        while True:
            try:
                outcome = await self._apply(event, record)
                if seen is None:
                    seen = outcome
                if await self._should_publish(outcome, record.partition, record.offset):
                    await publish_snapshots(
                        self.producer,
                        self.redis,
                        self.settings,
                        event,
                        partition=record.partition,
                        offset=record.offset,
                    )
            except Exception as exc:
                pending = await self._reload_script(exc)
                if pending is None:
                    continue
                if not _is_retryable(pending):
                    if pending is not exc:
                        raise pending from exc
                    raise
                self._log(
                    record,
                    "apply failed",
                    outcome="retry",
                    event_id=str(event.event_id),
                    level=logging.WARNING,
                    exc=pending,
                )
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.redis_backoff_max_s)
                continue
            if seen is None:
                msg = "apply finished without an outcome"
                raise RuntimeError(msg)
            if not logged:
                self._count(seen)
                self._log(
                    record,
                    "applied event" if seen == "applied" else "duplicate event",
                    outcome=seen,
                    event_id=str(event.event_id),
                )
                logged = True
            await self._commit(record, str(event.event_id))
            return

    async def heartbeat(self) -> None:
        """Refresh ``as_of_ms`` on caught-up partitions. Writer state only."""
        while True:
            try:
                await self.heartbeat_once()
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                logging.getLogger("features").warning("heartbeat failed", exc_info=True)
            await self.sleep(self.settings.heartbeat_interval_s)

    async def heartbeat_once(self) -> None:
        """Set ``as_of_ms`` to now on owned partitions that have reached the log end."""
        assignment = [
            tp for tp in self.consumer.assignment() if tp.topic == self.settings.raw_topic
        ]
        if not assignment:
            return
        ends = await self.consumer.end_offsets(assignment)
        now_ms = str(self.now_ms())
        for tp in assignment:
            position = await self.consumer.position(tp)
            if position >= ends[tp]:
                await _await(self.redis.hset(watermark_key(tp.partition), "as_of_ms", now_ms))

    async def _apply(self, event: TransactionEvent, record: ConsumerRecord) -> Outcome:
        if self._sha is None:
            self._sha = await load_script(self.redis)
        return await apply_event(
            self.redis,
            self._sha,
            event,
            partition=record.partition,
            offset=record.offset,
            now_ms=self.now_ms(),
        )

    async def _should_publish(self, outcome: Outcome, partition: int, offset: int) -> bool:
        if outcome == "applied":
            return True
        return await read_offset(self.redis, partition) == offset

    async def _reload_script(self, exc: Exception) -> Exception | None:
        """Reload after NOSCRIPT.

        ``None`` means the script is loaded and the apply should be retried
        immediately. Otherwise the returned error is what the caller retries
        or raises.
        """
        if not _is_noscript(exc):
            return exc
        try:
            self._sha = await load_script(self.redis)
        except Exception as load_exc:
            return load_exc
        return None

    async def _commit(self, record: ConsumerRecord, event_id: str | None) -> None:
        backoff = self.settings.redis_backoff_s
        offsets = {TopicPartition(record.topic, record.partition): record.offset + 1}
        while True:
            try:
                await self.consumer.commit(offsets)
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                self._log(
                    record,
                    "commit failed",
                    outcome="retry",
                    event_id=event_id,
                    level=logging.WARNING,
                    exc=exc,
                )
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.redis_backoff_max_s)

    async def _load_with_retry(self) -> None:
        async def load() -> None:
            self._sha = await load_script(self.redis)

        await self._retry(load, "redis script load failed")

    async def _retry(self, action: Callable[[], Awaitable[object]], message: str) -> None:
        backoff = self.settings.redis_backoff_s
        while True:
            try:
                await action()
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                logging.getLogger("features").warning(message, exc_info=_exc_info(exc))
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.redis_backoff_max_s)

    def _count(self, outcome: Outcome) -> None:
        if outcome == "applied":
            self.counters.applied += 1
        else:
            self.counters.dup += 1

    def _log(
        self,
        record: ConsumerRecord,
        message: str,
        *,
        outcome: str,
        event_id: str | None,
        level: int = logging.INFO,
        exc: Exception | None = None,
    ) -> None:
        logging.getLogger("features").log(
            level,
            message,
            extra={
                "event_id": event_id,
                "partition": record.partition,
                "offset": record.offset,
                "outcome": outcome,
                "applied": self.counters.applied,
                "dup": self.counters.dup,
                "skipped": self.counters.skipped,
            },
            exc_info=_exc_info(exc) if exc is not None else None,
        )


async def serve(settings: Settings) -> None:
    """Connect and run until the task is cancelled."""
    configure_logging()
    writer = await OnlineWriter.connect(settings)
    try:
        await writer.supervise()
    finally:
        await writer.aclose()


async def run(settings: Settings) -> None:
    """Run until SIGINT or SIGTERM."""
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(serve(settings), name="features-worker")
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
