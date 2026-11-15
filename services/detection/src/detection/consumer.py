"""Consume ``features.updates`` and write alerts. Commit after the database write."""

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from psycopg import AsyncConnection, InterfaceError, OperationalError
from psycopg.errors import UniqueViolation
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError

from detection.rules import RuleCache, apply_snapshot, load_rules
from detection.settings import Settings
from schemas import FeatureSnapshot

_LOG_FIELDS = ("event_id", "alert_id")
Sleep = Callable[[float], Awaitable[None]]


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with ``event_id`` and ``alert_id`` when set."""

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
    logger = logging.getLogger("detection")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


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


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, UniqueViolation):
        return True
    if isinstance(exc, (OperationalError, InterfaceError, TimeoutError, OSError)):
        return True
    return type(exc).__module__.startswith("aiokafka")


def _exc_info(exc: Exception) -> tuple[type[Exception], Exception, Any]:
    return (type(exc), exc, exc.__traceback__)


class DetectionConsumer:
    """Read feature snapshots, evaluate the rule cache, then commit.

    Consumption is at-least-once (ADR-003): the offset is committed after the
    alert insert. A repeated snapshot finds the open alert and inserts nothing.
    """

    def __init__(
        self,
        settings: Settings,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
        cache: RuleCache,
        consumer: AIOKafkaConsumer,
        *,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.settings = settings
        self.pool = pool
        self.cache = cache
        self.consumer = consumer
        self.sleep = sleep
        self._started = False
        self._closed = False

    @classmethod
    async def connect(
        cls,
        settings: Settings,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
        cache: RuleCache,
    ) -> "DetectionConsumer":
        """Build the consumer, connect to Kafka, and return it."""
        consumer = AIOKafkaConsumer(
            settings.updates_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            group_id=settings.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            client_id="detection",
        )
        service = cls(settings, pool, cache, consumer)
        try:
            await service.start()
        except BaseException:
            await service.aclose()
            raise
        return service

    async def start(self) -> None:
        """Connect the consumer. Retries while the broker is unreachable."""
        await self._retry(self.consumer.start, "kafka consumer connection failed")
        self._started = True

    async def aclose(self) -> None:
        """Stop the consumer. A second call does nothing."""
        if self._closed:
            return
        self._closed = True
        try:
            await self.consumer.stop()
        except Exception:
            logging.getLogger("detection").warning("kafka consumer stop failed", exc_info=True)

    async def ready(self) -> bool:
        """Return whether the consumer is started and the broker answers within 1 s.

        The consume loop is checked separately by ``/ready``. This probe asks
        the consumer client for metadata. ``AIOKafkaConsumer`` keeps that
        client on ``_client``.
        """
        if not self._started or self._closed:
            return False
        client = getattr(self.consumer, "_client", None)
        if client is None:
            client = getattr(self.consumer, "client", None)
        if client is None:
            return False
        try:
            updated = await asyncio.wait_for(client.force_metadata_update(), timeout=1)
        except Exception:
            return False
        return bool(updated)

    async def reload(self) -> None:
        """Replace the rule cache from Postgres."""
        self.cache.replace(await load_rules(self.pool))

    async def reload_with_retry(self) -> None:
        """Load rules before consuming. Retry while Postgres is unreachable."""
        await self._retry(self.reload, "rule load failed")

    async def reload_loop(self) -> None:
        """Reload rules on the configured interval. Keep the previous cache on failure."""
        while True:
            await self.sleep(self.settings.rule_reload_s)
            try:
                await self.reload()
            except Exception:
                logging.getLogger("detection").warning("rule reload failed", exc_info=True)

    async def run(self) -> None:
        """Refresh rules and consume until cancelled."""
        reload = asyncio.create_task(self.reload_loop(), name="detection-rules")
        consume = asyncio.create_task(self.consume(), name="detection-consume")
        try:
            await asyncio.gather(reload, consume)
        finally:
            reload.cancel()
            consume.cancel()
            await asyncio.gather(reload, consume, return_exceptions=True)

    async def consume(self) -> None:
        """Apply each record before reading the next."""
        async for record in self.consumer:
            await self.handle(record)

    async def handle(self, record: ConsumerRecord) -> None:
        """Evaluate one snapshot, write alerts, then commit."""
        payload = record.value
        if not isinstance(payload, bytes):
            self._log("skipped invalid record", event_id=None)
            await self._commit(record, None)
            return
        try:
            snapshot = FeatureSnapshot.from_bytes(payload)
        except ValidationError:
            event_id = event_id_from_bytes(payload)
            self._log("skipped invalid record", event_id=event_id)
            await self._commit(record, event_id)
            return
        alert_ids = await self._apply_with_retry(snapshot)
        for alert_id in alert_ids:
            self._log_fire(snapshot, alert_id)
        await self._commit(record, str(snapshot.event_id))

    async def _apply_with_retry(self, snapshot: FeatureSnapshot) -> tuple[UUID, ...]:
        backoff = self.settings.backoff_s
        while True:
            try:
                async with self.pool.connection() as conn:
                    return await apply_snapshot(conn, self.cache.rules, snapshot)
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                self._log(
                    "apply failed",
                    event_id=str(snapshot.event_id),
                    level=logging.WARNING,
                    exc=exc,
                )
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.backoff_max_s)

    async def _commit(self, record: ConsumerRecord, event_id: str | None) -> None:
        backoff = self.settings.backoff_s
        offsets = {TopicPartition(record.topic, record.partition): record.offset + 1}
        while True:
            try:
                await self.consumer.commit(offsets)
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                self._log(
                    "commit failed",
                    event_id=event_id,
                    level=logging.WARNING,
                    exc=exc,
                )
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.backoff_max_s)

    async def _retry(self, action: Callable[[], Awaitable[object]], message: str) -> None:
        backoff = self.settings.backoff_s
        while True:
            try:
                await action()
                return
            except Exception as exc:
                if not _is_retryable(exc):
                    raise
                logging.getLogger("detection").warning(message, exc_info=_exc_info(exc))
                await self.sleep(backoff)
                backoff = min(backoff * 2, self.settings.backoff_max_s)

    def _log(
        self,
        message: str,
        *,
        event_id: str | None,
        level: int = logging.INFO,
        exc: Exception | None = None,
    ) -> None:
        logging.getLogger("detection").log(
            level,
            message,
            extra={"event_id": event_id},
            exc_info=_exc_info(exc) if exc is not None else None,
        )

    def _log_fire(self, snapshot: FeatureSnapshot, alert_id: UUID) -> None:
        logging.getLogger("detection").info(
            "alert fired",
            extra={"event_id": str(snapshot.event_id), "alert_id": str(alert_id)},
        )
