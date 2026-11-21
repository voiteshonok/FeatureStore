"""ClickHouse outage and catch-up for online + offline writers (ADR-001, ADR-018)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import clickhouse_connect
import pytest
from aiokafka import AIOKafkaProducer
from prometheus_client import CollectorRegistry
from redis.asyncio import Redis
from stack import KafkaService, ensure_topics, kafka_container, unused_port, wait_for_kafka
from support import make_event
from testcontainers.community.clickhouse import ClickHouseContainer

from features.metrics import OfflineMetrics, offline_metrics
from features.offline import OfflineWriter
from features.settings import Settings
from features.windows import read_offset
from features.worker import serve
from schemas import TransactionEvent

pytestmark = pytest.mark.integration

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.3"
_REPO_ROOT = Path(__file__).resolve().parents[3]
CLICKHOUSE_INIT_DIR = _REPO_ROOT / "infra" / "clickhouse"
_SVC_OFFLINE_PASSWORD = "svc_offline_dev"  # noqa: S105
_N = 5
_WARMUP = 3
_PARTITIONS = 4


async def _wait_until(
    predicate: Callable[[], Awaitable[bool]],
    *,
    timeout_s: float = 60,
    interval_s: float = 0.1,
    message: str = "condition not met",
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval_s)
    raise TimeoutError(message)


def _lag_sum(metrics: OfflineMetrics) -> float:
    total = 0.0
    for family in metrics.consumer_lag.collect():
        for sample in family.samples:
            if sample.name == "offline_consumer_lag":
                total += float(sample.value)
    return total


def _count_events_final(client: clickhouse_connect.driver.Client) -> int:
    rows = client.query("SELECT count() FROM featurestore.events FINAL").result_rows
    return int(rows[0][0])


def _count_snapshots_final(client: clickhouse_connect.driver.Client) -> int:
    rows = client.query("SELECT count() FROM featurestore.feature_snapshots FINAL").result_rows
    return int(rows[0][0])


def _event_ids_final(client: clickhouse_connect.driver.Client) -> set[UUID]:
    rows = client.query("SELECT event_id FROM featurestore.events FINAL").result_rows
    found: set[UUID] = set()
    for row in rows:
        value = row[0]
        found.add(value if isinstance(value, UUID) else UUID(str(value)))
    return found


@pytest.fixture
def recovery_kafka() -> KafkaService:
    """Fresh broker per test so earlier recovery runs cannot leak event_ids."""
    host_port = unused_port()
    bootstrap = f"127.0.0.1:{host_port}"
    container = kafka_container(host_port)
    with container:
        wait_for_kafka(bootstrap)
        ensure_topics(bootstrap)
        yield KafkaService(container, bootstrap)


@pytest.fixture
def clickhouse_service():
    clickhouse = (
        ClickHouseContainer(
            CLICKHOUSE_IMAGE,
            username="featurestore",
            password="featurestore",  # noqa: S106
            dbname="featurestore",
        )
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_env("SVC_OFFLINE_PASSWORD", _SVC_OFFLINE_PASSWORD)
        .with_env("SVC_FEATURES_READ_PASSWORD", "svc_features_read_dev")
        .with_volume_mapping(str(CLICKHOUSE_INIT_DIR), "/docker-entrypoint-initdb.d", "ro")
    )
    with clickhouse:
        yield clickhouse


def _settings(bootstrap: str, clickhouse: ClickHouseContainer, redis_url: str) -> Settings:
    host = clickhouse.get_container_host_ip()
    port = int(clickhouse.get_exposed_port(8123))
    return Settings(
        kafka_bootstrap=bootstrap,
        redis_url=redis_url,
        clickhouse_url=f"http://{host}:{port}",
        clickhouse_user="svc_offline",
        clickhouse_password=_SVC_OFFLINE_PASSWORD,
        clickhouse_db="featurestore",
        n_partitions=_PARTITIONS,
        offline_batch_rows=50,
        offline_flush_interval_s=0.2,
        offline_backoff_s=0.05,
        offline_backoff_max_s=0.5,
        metrics_port=0,
    )


def _admin_client(
    clickhouse: ClickHouseContainer,
    *,
    send_receive_timeout: int = 30,
) -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),
        port=int(clickhouse.get_exposed_port(8123)),
        username=clickhouse.username,
        password=clickhouse.password,
        database="featurestore",
        send_receive_timeout=send_receive_timeout,
    )


def _make_events(prefix: str, count: int, base: datetime) -> list[TransactionEvent]:
    return [
        make_event(
            event_id=uuid4(),
            payment_id=f"{prefix}-pay-{index}",
            card_token=f"{prefix}-card-{index % 3}",
            merchant_id=f"{prefix}-m-{index % 2}",
            amount_minor=100 + index,
            event_time=base + timedelta(seconds=index),
            ingest_time=base + timedelta(seconds=index),
        )
        for index in range(count)
    ]


async def _produce_raw(
    bootstrap: str,
    topic: str,
    events: list[TransactionEvent],
) -> list[tuple[TransactionEvent, int, int]]:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    placed: list[tuple[TransactionEvent, int, int]] = []
    try:
        for event in events:
            meta = await producer.send_and_wait(
                topic,
                value=event.to_bytes(),
                key=event.card_token.encode("utf-8"),
            )
            placed.append((event, int(meta.partition), int(meta.offset)))
    finally:
        await producer.stop()
    return placed


async def _wait_online_applied(
    redis: Redis,
    partition: int,
    offset: int,
    *,
    timeout_s: float = 2.0,
) -> None:
    async def ready() -> bool:
        stored = await read_offset(redis, partition)
        return stored is not None and stored >= offset

    await _wait_until(
        ready,
        timeout_s=timeout_s,
        interval_s=0.05,
        message=(
            f"online-writer did not apply partition={partition} offset={offset} in {timeout_s}s"
        ),
    )


async def _stop_task(task: asyncio.Task[None]) -> None:
    if task.done():
        try:
            await task
        except (asyncio.CancelledError, Exception):
            return
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


async def _wait_caught_up(
    admin: clickhouse_connect.driver.Client,
    metrics: OfflineMetrics,
    expected_events: int,
) -> None:
    async def ready() -> bool:
        return _count_events_final(admin) >= expected_events and _lag_sum(metrics) == 0.0

    await _wait_until(
        ready,
        timeout_s=60,
        message=f"offline did not catch up to {expected_events} events with lag 0",
    )


@pytest.mark.asyncio
async def test_clickhouse_pause_online_stays_fresh_offline_catches_up(
    recovery_kafka: KafkaService,
    redis_url: str,
    clickhouse_service: ClickHouseContainer,
) -> None:
    bootstrap = recovery_kafka.bootstrap
    settings = _settings(bootstrap, clickhouse_service, redis_url)
    metrics = offline_metrics(CollectorRegistry())
    redis = Redis.from_url(redis_url, decode_responses=True)
    await redis.flushdb()
    admin = _admin_client(clickhouse_service)
    online_task = asyncio.create_task(serve(settings), name="online-recovery")
    offline = await OfflineWriter.connect(settings, metrics=metrics)
    offline_task = asyncio.create_task(offline.run(), name="offline-recovery")
    all_ids: set[UUID] = set()
    try:
        base = datetime.now(UTC).replace(microsecond=0)
        warm = _make_events("warm", _WARMUP, base)
        all_ids.update(event.event_id for event in warm)
        await _produce_raw(bootstrap, settings.raw_topic, warm)
        await _wait_caught_up(admin, metrics, _WARMUP)
        count_before_pause = _count_events_final(admin)
        lag_before = _lag_sum(metrics)

        clickhouse_service.get_wrapped_container().pause()
        paused = _make_events("pause", _N, base + timedelta(minutes=1))
        all_ids.update(event.event_id for event in paused)
        for _event, partition, offset in await _produce_raw(bootstrap, settings.raw_topic, paused):
            await _wait_online_applied(redis, partition, offset, timeout_s=2.0)

        async def lag_grew() -> bool:
            return _lag_sum(metrics) > lag_before

        await _wait_until(
            lag_grew,
            timeout_s=30,
            message="offline_consumer_lag did not grow while ClickHouse was paused",
        )
        # Do not query the paused container (testcontainers treats it as not running).
        # Lag growth + frozen pre-pause count prove no commit of the new ids.
        assert count_before_pause == _WARMUP

        clickhouse_service.get_wrapped_container().unpause()
        await _wait_caught_up(admin, metrics, _WARMUP + _N)
        assert _event_ids_final(admin) == all_ids
        assert _count_snapshots_final(admin) == 2 * (_WARMUP + _N)
    finally:
        await _stop_task(offline_task)
        await offline.aclose()
        await _stop_task(online_task)
        await redis.aclose()
        admin.close()


@pytest.mark.asyncio
async def test_offline_hard_cancel_with_buffer_then_restart_catches_up(
    recovery_kafka: KafkaService,
    redis_url: str,
    clickhouse_service: ClickHouseContainer,
) -> None:
    bootstrap = recovery_kafka.bootstrap
    settings = _settings(bootstrap, clickhouse_service, redis_url)
    metrics = offline_metrics(CollectorRegistry())
    redis = Redis.from_url(redis_url, decode_responses=True)
    await redis.flushdb()
    admin = _admin_client(clickhouse_service)
    online_task = asyncio.create_task(serve(settings), name="online-hard-cancel")
    offline = await OfflineWriter.connect(settings, metrics=metrics)
    offline_task = asyncio.create_task(offline.run(), name="offline-hard-cancel")
    all_ids: set[UUID] = set()
    try:
        base = datetime.now(UTC).replace(microsecond=0)
        warm = _make_events("hc-warm", _WARMUP, base)
        all_ids.update(event.event_id for event in warm)
        await _produce_raw(bootstrap, settings.raw_topic, warm)
        await _wait_caught_up(admin, metrics, _WARMUP)

        clickhouse_service.get_wrapped_container().pause()
        held = _make_events("hc-hold", _N, base + timedelta(minutes=1))
        all_ids.update(event.event_id for event in held)
        for _event, partition, offset in await _produce_raw(bootstrap, settings.raw_topic, held):
            await _wait_online_applied(redis, partition, offset, timeout_s=2.0)

        async def lag_grew() -> bool:
            return _lag_sum(metrics) > 0

        await _wait_until(
            lag_grew,
            timeout_s=30,
            message="offline did not buffer work before hard cancel",
        )

        # Kill mid-retry: cancel without flushing. Buffer is discarded; offsets stay
        # uncommitted. Stop the consumer only so the group is free (process death
        # would release it after session timeout — too slow for CI).
        offline_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await offline_task
        await offline.consumer.stop()

        clickhouse_service.get_wrapped_container().unpause()
        metrics2 = offline_metrics(CollectorRegistry())
        offline2 = await OfflineWriter.connect(settings, metrics=metrics2)
        offline2_task = asyncio.create_task(offline2.run(), name="offline-hard-cancel-2")
        try:
            await _wait_caught_up(admin, metrics2, _WARMUP + _N)
            assert _event_ids_final(admin) == all_ids
            assert _count_snapshots_final(admin) == 2 * (_WARMUP + _N)
        finally:
            await _stop_task(offline2_task)
            await offline2.aclose()
    finally:
        if not offline_task.done():
            await _stop_task(offline_task)
        await _stop_task(online_task)
        await redis.aclose()
        admin.close()
