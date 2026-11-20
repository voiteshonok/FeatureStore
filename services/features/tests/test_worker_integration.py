"""Kafka and Redis coverage for the online writer."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, OffsetSpec
from redis.asyncio import Redis
from support import make_event

from features.settings import Settings
from features.windows import (
    card_stream_features,
    merchant_features_key,
    minute_bucket,
    read_offset,
    stream_specs,
    to_epoch_ms,
)
from features.worker import serve
from schemas import FeatureSnapshot, TransactionEvent, partition_for

pytestmark = pytest.mark.integration

_PARTITIONS = 4
_PER_CARD = 25


def _cards() -> dict[int, str]:
    found: dict[int, str] = {}
    index = 0
    while len(found) < _PARTITIONS:
        token = f"card-{index}"
        found.setdefault(partition_for(token, _PARTITIONS), token)
        index += 1
    return found


def _minute_start() -> datetime:
    """Start of the current minute, so every event survives the 24 h trim."""
    now = datetime.now(UTC).replace(microsecond=0)
    return now.replace(second=0)


def _events(base: datetime) -> list[TransactionEvent]:
    events: list[TransactionEvent] = []
    for partition, token in sorted(_cards().items()):
        for index in range(_PER_CARD):
            when = base + timedelta(seconds=index)
            events.append(
                make_event(
                    card_token=token,
                    payment_id=f"pay-{partition}-{index}",
                    amount_minor=100 + index,
                    status="declined" if index % 5 == 0 else "approved",
                    event_time=when,
                    ingest_time=when,
                )
            )
    return events


def _group_offsets(bootstrap: str, group: str, topic: str) -> dict[int, int] | None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="features-offsets")
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
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="features-reset")
    try:
        specs = {
            TopicPartition(topic, partition): OffsetSpec.EARLIEST
            for partition in range(_PARTITIONS)
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


async def _wait_until_committed(
    task: asyncio.Task[None],
    bootstrap: str,
    group: str,
    topic: str,
    expected: dict[int, int],
) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if task.done():
            await task
        found = await asyncio.to_thread(_group_offsets, bootstrap, group, topic)
        caught_up = found is not None and all(
            found.get(partition) == offset for partition, offset in expected.items()
        )
        if caught_up:
            return
        await asyncio.sleep(0.2)
    msg = f"group {group} did not reach offsets {expected}"
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
    msg = f"could not reset {group} to the start of {topic}"
    raise TimeoutError(msg) from last_error


async def _produce(
    bootstrap: str,
    topic: str,
    events: list[TransactionEvent],
) -> dict[int, int]:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    last: dict[int, int] = {}
    try:
        for event in events:
            meta = await producer.send_and_wait(
                topic,
                value=event.to_bytes(),
                key=event.card_token.encode(),
            )
            assert meta.partition == partition_for(event.card_token, _PARTITIONS)
            last[int(meta.partition)] = int(meta.offset)
    finally:
        await producer.stop()
    return last


async def _snapshots(bootstrap: str, topic: str, expected: int) -> list[FeatureSnapshot]:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id="features-updates-test",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    found: list[FeatureSnapshot] = []
    try:
        deadline = time.monotonic() + 30
        while len(found) < expected and time.monotonic() < deadline:
            batches = await consumer.getmany(timeout_ms=1000, max_records=200)
            for records in batches.values():
                for record in records:
                    found.append(FeatureSnapshot.from_bytes(record.value))
    finally:
        await consumer.stop()
    return found


async def test_worker_is_replay_safe_and_publishes_snapshots(
    kafka: object,
    redis_url: str,
) -> None:
    bootstrap = kafka.bootstrap  # type: ignore[attr-defined]
    settings = Settings(kafka_bootstrap=bootstrap, redis_url=redis_url)
    assert settings.group_id == "online-writer"
    base = _minute_start()
    events = _events(base)
    last_offsets = await _produce(bootstrap, settings.raw_topic, events)
    assert set(last_offsets) == set(range(_PARTITIONS))
    committed = {partition: offset + 1 for partition, offset in last_offsets.items()}

    redis = Redis.from_url(redis_url, decode_responses=True)
    task = asyncio.create_task(serve(settings))
    try:
        await _wait_until_committed(
            task,
            bootstrap,
            settings.group_id,
            settings.raw_topic,
            committed,
        )
        for partition, offset in last_offsets.items():
            assert await read_offset(redis, partition) == offset

        query_ms = to_epoch_ms(base + timedelta(seconds=_PER_CARD - 1))
        by_card: dict[str, list[TransactionEvent]] = defaultdict(list)
        for event in events:
            by_card[event.card_token].append(event)

        for token, card_events in by_card.items():
            expected = sum(
                1
                for event in card_events
                if query_ms - 60_000 < to_epoch_ms(event.event_time) <= query_ms
            )
            features = await card_stream_features(redis, token, query_ms)
            assert features["card_cnt_1m"] == expected == _PER_CARD

        feat = merchant_features_key("m-1")
        minute = minute_bucket(to_epoch_ms(base))
        fields = await redis.hgetall(feat)
        assert int(fields[f"m:{minute}:c"]) == len(events)
        assert int(fields[f"m:{minute}:s"]) == sum(event.amount_minor for event in events)
        assert int(fields[f"m:{minute}:d"]) == sum(event.status == "declined" for event in events)

        snapshots = await _snapshots(bootstrap, settings.updates_topic, len(events) * 2)
        assert len(snapshots) == len(events) * 2
        card_names = {spec.name for spec in stream_specs("card")}
        merchant_names = {spec.name for spec in stream_specs("merchant")}
        assert "geo_velocity_kmh" not in card_names
        seen: dict[str, FeatureSnapshot] = {}
        for snapshot in snapshots:
            if snapshot.entity_type == "card":
                assert set(snapshot.features) == card_names
                current = seen.get(snapshot.entity_id)
                if current is None or snapshot.source_offset > current.source_offset:
                    seen[snapshot.entity_id] = snapshot
            else:
                assert snapshot.entity_type == "merchant"
                assert set(snapshot.features) == merchant_names
        for token, card_events in by_card.items():
            last = seen[token]
            assert last.features["card_cnt_1m"] == _PER_CARD
            assert last.features["card_sum_1m"] == sum(event.amount_minor for event in card_events)

        before = await redis.hgetall(feat)
        await _stop(task)
        await _reset_when_idle(bootstrap, settings.group_id, settings.raw_topic)
        task = asyncio.create_task(serve(settings))
        await _wait_until_committed(
            task,
            bootstrap,
            settings.group_id,
            settings.raw_topic,
            committed,
        )
        assert await redis.hgetall(feat) == before
    finally:
        if not task.done():
            await _stop(task)
        await redis.aclose()
