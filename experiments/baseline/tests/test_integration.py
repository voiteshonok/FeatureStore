"""Kafka and Postgres coverage for the SQL stand."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from baseline_containers import (
    PARTITIONS,
    POSTGRES_IMAGE,
    TOPIC,
    KafkaService,
    PostgresService,
    apply_schema,
    dsn,
    kafka_container,
    unused_port,
    wait_for_kafka,
)
from httpx import ASGITransport, AsyncClient
from kafka import TopicPartition as AdminPartition
from kafka.admin import KafkaAdminClient, OffsetSpec
from testcontainers.community.postgres import PostgresContainer

from baseline.api import create_app
from baseline.metrics import api_metrics, writer_metrics
from baseline.settings import Settings
from baseline.windows import in_card_window, in_merchant_window
from baseline.writer import serve
from schemas import CATALOG, TransactionEvent, for_entity

pytestmark = pytest.mark.integration

_T = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
_STREAM = {spec.name for spec in CATALOG if spec.kind == "stream"}
_FORBIDDEN = (
    "SELECT * FROM features.feature_definitions",
    "SELECT * FROM ingestion.idempotency_keys",
    "SELECT * FROM detection.alerts",
    "SELECT * FROM ref.merchants",
    "SELECT * FROM ops.operators",
)


@pytest.fixture(scope="module")
def kafka() -> Iterator[KafkaService]:
    host_port = unused_port()
    bootstrap = f"127.0.0.1:{host_port}"
    container = kafka_container(host_port)
    with container:
        wait_for_kafka(bootstrap)
        yield KafkaService(container, bootstrap)


@pytest.fixture(scope="module")
def postgres() -> Iterator[PostgresService]:
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as container:
        super_dsn = dsn(container, user=container.username, password=container.password)
        apply_schema(super_dsn)
        yield PostgresService(
            baseline_dsn=dsn(
                container,
                user="svc_baseline",
                password="svc_baseline_dev",  # noqa: S106
            )
        )


def _event(overrides: dict[str, object] | None = None) -> TransactionEvent:
    payload: dict[str, object] = {
        "event_id": str(uuid4()),
        "payment_id": "pay-1",
        "merchant_id": "m1",
        "amount_minor": 100,
        "currency": "EUR",
        "status": "approved",
        "event_time": _T,
        "card_token": "c1",
        "ingest_time": _T,
        "lat": 52.5,
        "lon": 13.4,
    }
    if overrides:
        payload.update(overrides)
    return TransactionEvent.model_validate(payload)


def _initial_events() -> list[TransactionEvent]:
    return [
        _event({"amount_minor": 10, "event_time": _T - timedelta(seconds=30)}),
        _event({"amount_minor": 20, "event_time": _T - timedelta(minutes=1)}),
        _event(
            {
                "amount_minor": 40,
                "status": "declined",
                "event_time": _T - timedelta(hours=1),
            }
        ),
        _event({"amount_minor": 80, "event_time": _T - timedelta(hours=24)}),
        _event({"amount_minor": 15, "event_time": _T - timedelta(minutes=1439)}),
        _event(
            {
                "card_token": "c2",
                "merchant_id": "m2",
                "amount_minor": 5,
                "status": "declined",
                "event_time": _T - timedelta(seconds=10),
            }
        ),
    ]


def _expected(events: list[TransactionEvent], card: str, merchant: str) -> dict[str, int | float]:
    card_rows = [event for event in events if event.card_token == card]
    merchant_rows = [event for event in events if event.merchant_id == merchant]
    found: dict[str, int | float] = {}
    for spec in for_entity("card"):
        if spec.kind != "stream" or spec.window is None:
            continue
        chosen = [event for event in card_rows if in_card_window(event.event_time, _T, spec.window)]
        if spec.agg == "count":
            found[spec.name] = len(chosen)
        elif spec.agg == "sum":
            found[spec.name] = sum(event.amount_minor for event in chosen)
    chosen_merchant = [event for event in merchant_rows if in_merchant_window(event.event_time, _T)]
    count = len(chosen_merchant)
    declined = sum(event.status == "declined" for event in chosen_merchant)
    found["merchant_cnt_24h"] = count
    found["merchant_decline_rate_24h"] = 0.0 if count == 0 else declined / count
    return found


def _assert_features(
    body: dict[str, object],
    events: list[TransactionEvent],
    card: str,
    merchant: str,
) -> None:
    raw = body["features"]
    assert isinstance(raw, dict)
    assert "geo_velocity_kmh" in raw
    expected = _expected(events, card, merchant)
    for name, value in raw.items():
        assert isinstance(value, dict)
        assert value["status"] in {"fresh", "default"}
        if name not in _STREAM:
            continue
        if name == "merchant_decline_rate_24h":
            assert value["value"] == pytest.approx(expected[name])
        else:
            assert value["value"] == expected[name]
        populated_name = "card_cnt_24h" if name.startswith("card_") else "merchant_cnt_24h"
        populated = expected[populated_name] != 0
        assert value["status"] == ("fresh" if populated else "default")


async def _produce(bootstrap: str, events: list[TransactionEvent]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for event in events:
            await producer.send_and_wait(
                TOPIC,
                value=event.to_bytes(),
                key=event.card_token.encode(),
            )
    finally:
        await producer.stop()


async def _produce_raw(bootstrap: str, payloads: list[bytes]) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for payload in payloads:
            await producer.send_and_wait(TOPIC, value=payload, key=b"c1")
    finally:
        await producer.stop()


def _read_count(database_url: str, sql: str) -> int:
    with psycopg.connect(database_url) as conn:
        row = conn.execute(sql).fetchone()
    if row is None:
        return 0
    return int(row[0])


async def _count(database_url: str) -> int:
    return await asyncio.to_thread(
        _read_count,
        database_url,
        "SELECT count(*) FROM baseline.transactions",
    )


async def _distinct(database_url: str) -> int:
    return await asyncio.to_thread(
        _read_count,
        database_url,
        "SELECT count(DISTINCT event_id) FROM baseline.transactions",
    )


def _group_offsets(bootstrap: str, group: str, topic: str) -> dict[int, int] | None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="baseline-offsets")
    try:
        try:
            fetched = admin.list_group_offsets(group)
        except Exception:
            return None
    finally:
        admin.close()
    offsets = fetched.get(group, {})
    found: dict[int, int] = {}
    for tp, meta in offsets.items():
        if tp.topic == topic:
            found[int(tp.partition)] = int(meta.offset)
    return found


def _reset_earliest(bootstrap: str, group: str, topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="baseline-reset")
    try:
        specs = {
            AdminPartition(topic, partition): OffsetSpec.EARLIEST for partition in range(PARTITIONS)
        }
        admin.reset_group_offsets(group, specs)
    finally:
        admin.close()


async def _stop(task: asyncio.Task[None]) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


async def _wait_count(task: asyncio.Task[None], database_url: str, expected: int) -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if task.done():
            await task
        if await _count(database_url) == expected:
            return
        await asyncio.sleep(0.2)
    msg = f"row count did not reach {expected}"
    raise TimeoutError(msg)


async def _wait_committed(task: asyncio.Task[None], bootstrap: str, group: str, topic: str) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    partitions = [TopicPartition(topic, index) for index in range(PARTITIONS)]
    consumer.assign(partitions)
    await consumer.start()
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if task.done():
                await task
            ends = await consumer.end_offsets(partitions)
            found = await asyncio.to_thread(_group_offsets, bootstrap, group, topic)
            caught_up = found is not None and all(
                found.get(tp.partition, 0) >= int(ends[tp]) for tp in partitions
            )
            if caught_up:
                return
            await asyncio.sleep(0.2)
    finally:
        await consumer.stop()
    msg = f"group {group} did not catch up"
    raise TimeoutError(msg)


async def _reset_when_idle(bootstrap: str, group: str, topic: str) -> None:
    deadline = time.monotonic() + 30
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            await asyncio.to_thread(_reset_earliest, bootstrap, group, topic)
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.5)
        else:
            return
    msg = f"could not reset {group}"
    raise TimeoutError(msg) from last_error


async def _post(client: AsyncClient, card: str, merchant: str) -> dict[str, object]:
    response = await client.post(
        "/v1/features",
        json={
            "card_token": card,
            "merchant_id": merchant,
            "ts": _T.isoformat(),
            "lat": 52.5,
            "lon": 13.4,
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def test_svc_baseline_cannot_read_other_schemas(postgres: PostgresService) -> None:
    with psycopg.connect(postgres.baseline_dsn, autocommit=True) as conn:
        for statement in _FORBIDDEN:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(statement)
        row = conn.execute("SELECT count(*) FROM baseline.transactions").fetchone()
    assert row is not None


async def test_writer_matches_the_event_list_and_survives_replay(
    kafka: KafkaService,
    postgres: PostgresService,
) -> None:
    captured: list[logging.LogRecord] = []

    class _Grab(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    logger = logging.getLogger("baseline")
    handler = _Grab()
    logger.addHandler(handler)
    settings = Settings(
        kafka_bootstrap=kafka.bootstrap,
        database_url=postgres.baseline_dsn,
        metrics_port=0,
        pool_min_size=1,
        pool_max_size=2,
    )
    assert settings.group_id == "baseline-writer"
    stored = _initial_events()
    await _produce(kafka.bootstrap, stored)
    task = asyncio.create_task(serve(settings, metrics=writer_metrics()))
    try:
        await _wait_count(task, postgres.baseline_dsn, len(stored))
        await _wait_committed(task, kafka.bootstrap, settings.group_id, settings.raw_topic)
        app = create_app(settings, metrics=api_metrics(), now=lambda: _T)
        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                first = await _post(client, "c1", "m1")
                _assert_features(first, stored, "c1", "m1")
                features = first["features"]
                assert isinstance(features, dict)
                minute = features["card_cnt_1m"]
                hour = features["card_sum_1h"]
                day = features["card_sum_24h"]
                rate = features["merchant_decline_rate_24h"]
                assert isinstance(minute, dict)
                assert isinstance(hour, dict)
                assert isinstance(day, dict)
                assert isinstance(rate, dict)
                assert minute["value"] == 1
                assert hour["value"] == 30
                assert day["value"] == 85
                assert rate["value"] == pytest.approx(0.25)

                await _produce(kafka.bootstrap, stored)
                await _wait_committed(task, kafka.bootstrap, settings.group_id, settings.raw_topic)
                assert await _count(postgres.baseline_dsn) == len(stored)
                _assert_features(await _post(client, "c1", "m1"), stored, "c1", "m1")

                await _produce_raw(
                    kafka.bootstrap,
                    [
                        b"not-json",
                        (
                            b'{"schema_version":1,"event_id":"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",'
                            b'"payment_id":"p","merchant_id":"m1","amount_minor":0,"currency":"EUR",'
                            b'"status":"approved","event_time":"2026-06-01T12:00:00Z",'
                            b'"card_token":"c1","ingest_time":"2026-06-01T12:00:00Z"}'
                        ),
                    ],
                )
                await _wait_committed(task, kafka.bootstrap, settings.group_id, settings.raw_topic)
                assert await _count(postgres.baseline_dsn) == len(stored)
                skipped = sum(
                    record.getMessage() == "skipped invalid record" for record in captured
                )
                assert skipped == 2

                await _stop(task)
                extra = [
                    _event({"amount_minor": 7, "event_time": _T - timedelta(seconds=1)}),
                    _event(
                        {
                            "card_token": "c3",
                            "amount_minor": 9,
                            "status": "declined",
                            "event_time": _T - timedelta(seconds=2),
                        }
                    ),
                ]
                stored.extend(extra)
                await _produce(kafka.bootstrap, extra)
                task = asyncio.create_task(serve(settings, metrics=writer_metrics()))
                await _wait_count(task, postgres.baseline_dsn, len(stored))
                await _wait_committed(task, kafka.bootstrap, settings.group_id, settings.raw_topic)

                await _stop(task)
                await _reset_when_idle(kafka.bootstrap, settings.group_id, settings.raw_topic)
                task = asyncio.create_task(serve(settings, metrics=writer_metrics()))
                await _wait_committed(task, kafka.bootstrap, settings.group_id, settings.raw_topic)
                assert await _count(postgres.baseline_dsn) == len(stored)
                assert await _distinct(postgres.baseline_dsn) == len(stored)

                _assert_features(await _post(client, "c1", "m1"), stored, "c1", "m1")
                _assert_features(await _post(client, "c2", "m2"), stored, "c2", "m2")
                _assert_features(
                    await _post(client, "c-missing", "m-missing"),
                    stored,
                    "c-missing",
                    "m-missing",
                )
                ready = await client.get("/ready")
                assert ready.status_code == 200
                assert ready.json()["postgres"] == "ok"
    finally:
        logger.removeHandler(handler)
        if not task.done():
            await _stop(task)
