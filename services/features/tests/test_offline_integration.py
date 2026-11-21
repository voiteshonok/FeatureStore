"""Kafka + ClickHouse coverage for the offline writer (ADR-018)."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import pytest
from aiokafka import AIOKafkaProducer
from stack import KafkaService, kafka_container, unused_port, wait_for_kafka
from support import make_event
from testcontainers.community.clickhouse import ClickHouseContainer

from features.offline import OfflineWriter
from features.settings import Settings
from schemas import FeatureSnapshot, TransactionEvent

pytestmark = pytest.mark.integration

CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.3"
_REPO_ROOT = Path(__file__).resolve().parents[3]
CLICKHOUSE_INIT_DIR = _REPO_ROOT / "infra" / "clickhouse"
_SVC_OFFLINE_PASSWORD = "svc_offline_dev"  # noqa: S105
_N = 5


def _snapshots(event: TransactionEvent, *, partition: int, offset: int) -> list[FeatureSnapshot]:
    features_card = {
        "card_cnt_1m": 1.0,
        "card_cnt_1h": 1.0,
        "card_cnt_24h": 1.0,
        "card_sum_1m": float(event.amount_minor),
        "card_sum_1h": float(event.amount_minor),
        "card_sum_24h": float(event.amount_minor),
    }
    features_merchant = {
        "merchant_cnt_24h": 1.0,
        "merchant_decline_rate_24h": 0.0,
    }
    return [
        FeatureSnapshot(
            entity_type="card",
            entity_id=event.card_token,
            event_id=event.event_id,
            event_time=event.event_time,
            ingest_time=event.ingest_time,
            features=features_card,
            source_partition=partition,
            source_offset=offset,
        ),
        FeatureSnapshot(
            entity_type="merchant",
            entity_id=event.merchant_id,
            event_id=event.event_id,
            event_time=event.event_time,
            ingest_time=event.ingest_time,
            features=features_merchant,
            source_partition=partition,
            source_offset=offset,
        ),
    ]


async def _produce(
    bootstrap: str,
    events: list[TransactionEvent],
    settings: Settings,
) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, acks="all")
    await producer.start()
    try:
        for index, event in enumerate(events):
            await producer.send_and_wait(
                settings.raw_topic,
                value=event.to_bytes(),
                key=event.card_token.encode("utf-8"),
            )
            for snapshot in _snapshots(event, partition=0, offset=index):
                await producer.send_and_wait(
                    settings.updates_topic,
                    value=snapshot.to_bytes(),
                    key=snapshot.entity_id.encode("utf-8"),
                )
    finally:
        await producer.stop()


def _count_events_final(client: clickhouse_connect.driver.Client) -> int:
    rows = client.query("SELECT count() FROM featurestore.events FINAL").result_rows
    return int(rows[0][0])


def _count_snapshots_final(client: clickhouse_connect.driver.Client) -> int:
    rows = client.query("SELECT count() FROM featurestore.feature_snapshots FINAL").result_rows
    return int(rows[0][0])


async def _wait_events(
    client: clickhouse_connect.driver.Client,
    expected: int,
    *,
    timeout_s: float = 60,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if _count_events_final(client) >= expected:
            return
        await asyncio.sleep(0.25)
    msg = f"expected {expected} events FINAL, still short after {timeout_s}s"
    raise TimeoutError(msg)


@pytest.fixture(scope="module")
def kafka_bootstrap() -> str:
    host_port = unused_port()
    with kafka_container(host_port) as container:
        bootstrap = f"127.0.0.1:{host_port}"
        wait_for_kafka(bootstrap)
        yield bootstrap
        _ = KafkaService(container=container, bootstrap=bootstrap)


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


def _settings(bootstrap: str, clickhouse: ClickHouseContainer) -> Settings:
    host = clickhouse.get_container_host_ip()
    port = int(clickhouse.get_exposed_port(8123))
    return Settings(
        kafka_bootstrap=bootstrap,
        clickhouse_url=f"http://{host}:{port}",
        clickhouse_user="svc_offline",
        clickhouse_password=_SVC_OFFLINE_PASSWORD,
        clickhouse_db="featurestore",
        offline_batch_rows=100,
        offline_flush_interval_s=0.2,
        offline_backoff_s=0.05,
        offline_backoff_max_s=0.5,
        metrics_port=0,
    )


def _admin_client(clickhouse: ClickHouseContainer):
    return clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),
        port=int(clickhouse.get_exposed_port(8123)),
        username=clickhouse.username,
        password=clickhouse.password,
        database="featurestore",
    )


@pytest.mark.asyncio
async def test_offline_writer_persists_events_and_snapshots(
    kafka_bootstrap: str,
    clickhouse_service: ClickHouseContainer,
) -> None:
    settings = _settings(kafka_bootstrap, clickhouse_service)
    base = datetime.now(UTC).replace(microsecond=0)
    events = [
        make_event(
            event_id=uuid4(),
            payment_id=f"pay-{index}",
            card_token=f"card-{index}",
            merchant_id=f"m-{index}",
            amount_minor=100 + index,
            event_time=base + timedelta(seconds=index),
            ingest_time=base + timedelta(seconds=index),
        )
        for index in range(_N)
    ]
    await _produce(kafka_bootstrap, events, settings)

    writer = await OfflineWriter.connect(settings)
    task = asyncio.create_task(writer.run(), name="offline-int")
    admin = _admin_client(clickhouse_service)
    try:
        await _wait_events(admin, _N)
        rows = admin.query(
            "SELECT event_id, payment_id, amount_minor FROM featurestore.events FINAL "
            "ORDER BY payment_id"
        ).result_rows
        assert len(rows) == _N
        by_payment = {row[1]: row for row in rows}
        for event in events:
            stored = by_payment[event.payment_id]
            assert stored[0] == event.event_id
            assert stored[2] == event.amount_minor
        assert _count_snapshots_final(admin) == 2 * _N
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await writer.aclose()
        admin.close()


@pytest.mark.asyncio
async def test_offline_writer_redelivery_after_commit_failure_keeps_final_count(
    kafka_bootstrap: str,
    clickhouse_service: ClickHouseContainer,
) -> None:
    settings = _settings(kafka_bootstrap, clickhouse_service)
    base = datetime.now(UTC).replace(microsecond=0)
    events = [
        make_event(
            event_id=uuid4(),
            payment_id=f"retry-pay-{index}",
            card_token=f"retry-card-{index}",
            merchant_id=f"retry-m-{index}",
            amount_minor=200 + index,
            event_time=base + timedelta(seconds=index),
            ingest_time=base + timedelta(seconds=index),
        )
        for index in range(_N)
    ]
    await _produce(kafka_bootstrap, events, settings)

    writer = await OfflineWriter.connect(settings)

    async def crash_after_insert(_offsets: dict) -> None:
        raise RuntimeError("crash between insert and commit")

    writer.consumer.commit = crash_after_insert  # type: ignore[method-assign]
    task = asyncio.create_task(writer.run(), name="offline-redeliver")
    admin = _admin_client(clickhouse_service)
    try:
        await _wait_events(admin, _N)
        # Insert succeeded; commit hook aborts the task without advancing offsets.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if task.done():
                with pytest.raises(RuntimeError, match="crash between insert"):
                    await task
                break
            await asyncio.sleep(0.1)
        else:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            raise TimeoutError("writer did not crash after insert")
    finally:
        await writer.aclose()

    # Restart from the same offsets: redelivery collapses under FINAL.
    writer2 = await OfflineWriter.connect(settings)
    task2 = asyncio.create_task(writer2.run(), name="offline-redeliver-2")
    try:
        await _wait_events(admin, _N)
        await asyncio.sleep(1.0)
        assert _count_events_final(admin) == _N
    finally:
        task2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task2
        await writer2.aclose()
        admin.close()
