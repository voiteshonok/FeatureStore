"""Compose proof that ClickHouse outage does not break Redis freshness (ADR-001/018).

Requires ``make secret`` and ``make up``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import clickhouse_connect
import httpx
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from schemas import FeatureStatus, TransactionEvent, for_entity
from simulator.sender import run_sender

pytestmark = pytest.mark.e2e

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = ["docker", "compose", "-f", str(_ROOT / "infra" / "docker-compose.yml")]
_API = "http://localhost:8001"
_INGESTION = "http://localhost:8000"
_BOOTSTRAP = "localhost:29092"
_TOPIC = "transactions.raw"
_UPDATES = "features.updates"
_PARTITIONS = 4
_SEED_DURATION_S = 5.0
_DURATION_S = 120.0
_TPS = 50.0
_STOP_AT_S = 30.0
_START_AT_S = 90.0


def _compose(*args: str) -> None:
    subprocess.run([*_COMPOSE, *args], check=True)  # noqa: S603


def _group_offsets(group: str, topic: str) -> dict[int, int] | None:
    admin = KafkaAdminClient(bootstrap_servers=_BOOTSTRAP, client_id="stage4-offsets")
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


def _caught_up(committed: dict[int, int] | None, ends: dict[int, int]) -> bool:
    if committed is None:
        return False
    return all(committed.get(partition, 0) >= end for partition, end in ends.items())


async def _wait_until(
    predicate,
    *,
    timeout_s: float,
    interval_s: float = 0.25,
    message: str,
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval_s)
    raise TimeoutError(message)


async def _read_raw_events() -> list[TransactionEvent]:
    consumer = AIOKafkaConsumer(bootstrap_servers=_BOOTSTRAP, enable_auto_commit=False)
    partitions = [TopicPartition(_TOPIC, index) for index in range(_PARTITIONS)]
    consumer.assign(partitions)
    await consumer.start()
    try:
        await consumer.seek_to_beginning(*partitions)
        events: list[TransactionEvent] = []
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=500)
            for records in batches.values():
                for record in records:
                    if isinstance(record.value, bytes):
                        events.append(TransactionEvent.from_bytes(record.value))
            if not batches:
                ends = await consumer.end_offsets(partitions)
                caught_up = True
                for partition in partitions:
                    if await consumer.position(partition) < ends[partition]:
                        caught_up = False
                        break
                if caught_up:
                    return events
    finally:
        await consumer.stop()


def _offline_password() -> str:
    password = os.environ.get("SVC_OFFLINE_PASSWORD")
    if not password:
        pytest.fail("SVC_OFFLINE_PASSWORD is unset; run make secret")
    return password


def _clickhouse_database() -> str:
    database = os.environ.get("CLICKHOUSE_DB", "featurestore")
    if not database.isidentifier():
        pytest.fail(f"invalid ClickHouse database name: {database!r}")
    return database


def _clickhouse_client(
    *,
    connect_timeout: int | None = None,
    send_receive_timeout: int | None = None,
) -> clickhouse_connect.driver.Client:
    password = _offline_password()
    database = _clickhouse_database()
    if connect_timeout is None and send_receive_timeout is None:
        return clickhouse_connect.get_client(
            host="127.0.0.1",
            port=8123,
            username="svc_offline",
            password=password,
            database=database,
        )
    return clickhouse_connect.get_client(
        host="127.0.0.1",
        port=8123,
        username="svc_offline",
        password=password,
        database=database,
        connect_timeout=10 if connect_timeout is None else connect_timeout,
        send_receive_timeout=300 if send_receive_timeout is None else send_receive_timeout,
    )


async def _wait_clickhouse_ready(*, timeout_s: float = 60) -> None:
    # Fail before the poll. The loop treats connection errors as "not ready".
    _offline_password()
    _clickhouse_database()

    async def ready() -> bool:
        try:
            client = _clickhouse_client(connect_timeout=2, send_receive_timeout=2)
            try:
                client.query("SELECT 1")
            finally:
                client.close()
            return True
        except Exception:
            return False

    await _wait_until(ready, timeout_s=timeout_s, message="ClickHouse did not become ready")


async def _wait_offline_caught_up(*, timeout_s: float = 60) -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=_BOOTSTRAP, enable_auto_commit=False)
    raw_parts = [TopicPartition(_TOPIC, index) for index in range(_PARTITIONS)]
    upd_parts = [TopicPartition(_UPDATES, index) for index in range(_PARTITIONS)]
    consumer.assign([*raw_parts, *upd_parts])
    await consumer.start()
    try:

        async def ready() -> bool:
            ends = await consumer.end_offsets([*raw_parts, *upd_parts])
            raw_ends = {tp.partition: int(ends[tp]) for tp in raw_parts}
            upd_ends = {tp.partition: int(ends[tp]) for tp in upd_parts}
            raw = await asyncio.to_thread(_group_offsets, "offline-writer", _TOPIC)
            updates = await asyncio.to_thread(_group_offsets, "offline-writer", _UPDATES)
            return _caught_up(raw, raw_ends) and _caught_up(updates, upd_ends)

        await _wait_until(
            ready,
            timeout_s=timeout_s,
            message="offline-writer did not reach lag 0 on both topics",
        )
    finally:
        await consumer.stop()


def _ch_event_ids() -> set[UUID]:
    database = _clickhouse_database()
    client = _clickhouse_client()
    try:
        # database is an identifier from the environment, same check as pan_scan.
        rows = client.query(f"SELECT event_id FROM {database}.events FINAL").result_rows  # noqa: S608
        found: set[UUID] = set()
        for row in rows:
            value = row[0]
            found.add(value if isinstance(value, UUID) else UUID(str(value)))
        return found
    finally:
        client.close()


async def _post_features(
    client: httpx.AsyncClient,
    *,
    card_token: str,
    merchant_id: str,
    lat: float | None,
    lon: float | None,
) -> dict[str, object]:
    body: dict[str, object] = {
        "card_token": card_token,
        "merchant_id": merchant_id,
        "ts": datetime.now(UTC).isoformat(),
    }
    if lat is not None and lon is not None:
        body["lat"] = lat
        body["lon"] = lon
    response = await client.post("/v1/features", json=body)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _stream_statuses(body: dict[str, object]) -> dict[str, str]:
    features = body["features"]
    assert isinstance(features, dict)
    statuses: dict[str, str] = {}
    for name, item in features.items():
        assert isinstance(name, str)
        assert isinstance(item, dict)
        status = item["status"]
        assert isinstance(status, str)
        statuses[name] = status
    return statuses


def _pick_probe(events: list[TransactionEvent]) -> tuple[str, str, float | None, float | None]:
    counts: dict[str, int] = {}
    for event in events:
        counts[event.card_token] = counts.get(event.card_token, 0) + 1
    probe_card = max(counts, key=lambda token: counts[token])
    latest = max(
        (event for event in events if event.card_token == probe_card),
        key=lambda event: event.event_time,
    )
    return probe_card, latest.merchant_id, latest.lat, latest.lon


@pytest.mark.asyncio
async def test_stage4_clickhouse_outage_keeps_features_fresh() -> None:
    card_names = {spec.name for spec in for_entity("card") if spec.kind == "stream"}
    seed = await run_sender(
        url=_INGESTION,
        tps=_TPS,
        duration_s=_SEED_DURATION_S,
        seed=40,
        run_id=str(uuid4()),
        report=False,
    )
    assert seed.accepted > 0

    events = await _read_raw_events()
    assert events, "seed traffic did not land in Kafka"
    probe_card, probe_merchant, probe_lat, probe_lon = _pick_probe(events)

    started = time.monotonic()
    sender = asyncio.create_task(
        run_sender(
            url=_INGESTION,
            tps=_TPS,
            duration_s=_DURATION_S,
            seed=41,
            run_id=str(uuid4()),
            report=False,
        ),
        name="stage4-sender",
    )
    ch_stopped = False
    ch_started = False

    async with httpx.AsyncClient(base_url=_API, timeout=10) as client:
        try:
            while True:
                elapsed = time.monotonic() - started
                if elapsed >= _STOP_AT_S and not ch_stopped:
                    _compose("pause", "clickhouse")
                    ch_stopped = True
                if elapsed >= _START_AT_S and not ch_started:
                    _compose("unpause", "clickhouse")
                    await _wait_clickhouse_ready()
                    ch_started = True

                body = await _post_features(
                    client,
                    card_token=probe_card,
                    merchant_id=probe_merchant,
                    lat=probe_lat,
                    lon=probe_lon,
                )
                statuses = _stream_statuses(body)
                for name in card_names:
                    assert statuses[name] == FeatureStatus.fresh, (
                        f"{name} was {statuses[name]} at t={elapsed:.1f}s"
                    )

                if sender.done() and ch_started:
                    break
                if elapsed > _DURATION_S + 30:
                    break
                await asyncio.sleep(0.5)

            stats = await sender
            assert stats.accepted > 0
            assert ch_stopped and ch_started
        finally:
            if not ch_started:
                with contextlib.suppress(Exception):
                    _compose("unpause", "clickhouse")
            if not sender.done():
                sender.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await sender

    await _wait_offline_caught_up(timeout_s=120)
    kafka_ids = {event.event_id for event in await _read_raw_events()}
    assert kafka_ids
    assert _ch_event_ids() == kafka_ids
