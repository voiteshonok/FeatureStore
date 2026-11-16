"""Feature API freshness on the compose stack.

Runs after the stage 2 burst: ``transactions.raw`` may already hold events.
Requires ``make secret`` and ``make up``. The baseline comparison also requires
``make baseline-up``.
"""

from __future__ import annotations

import asyncio
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from features.windows import to_epoch_ms
from schemas import CATALOG, FeatureStatus, TransactionEvent, for_entity
from simulator.sender import run_sender

pytestmark = pytest.mark.e2e

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = ["docker", "compose", "-f", str(_ROOT / "infra" / "docker-compose.yml")]
_API = "http://localhost:8001"
_BASELINE = "http://localhost:8003"
_INGESTION = "http://localhost:8000"
_BOOTSTRAP = "localhost:29092"
_TOPIC = "transactions.raw"
_PARTITIONS = 4
_HOUR = timedelta(hours=1)
_DAY = timedelta(hours=24)
_STREAM = {spec.name for spec in CATALOG if spec.kind == "stream"}


def _compose(*args: str) -> None:
    subprocess.run([*_COMPOSE, *args], check=True)  # noqa: S603


async def _read_events() -> list[TransactionEvent]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=_BOOTSTRAP,
        enable_auto_commit=False,
    )
    partitions = [TopicPartition(_TOPIC, index) for index in range(_PARTITIONS)]
    consumer.assign(partitions)
    await consumer.start()
    try:
        await consumer.seek_to_beginning(*partitions)
        events: list[TransactionEvent] = []
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=500)
            for records in batches.values():
                events.extend(TransactionEvent.from_bytes(record.value) for record in records)
            if not batches:
                ends = await consumer.end_offsets(partitions)
                caught_up = True
                for partition in partitions:
                    if await consumer.position(partition) < ends[partition]:
                        caught_up = False
                if caught_up:
                    return events
    finally:
        await consumer.stop()


def _minute(moment: datetime) -> int:
    return to_epoch_ms(moment) // 60_000


def _card_sum(events: list[TransactionEvent], now: datetime, window: timedelta) -> tuple[int, int]:
    chosen = [event for event in events if now - window < event.event_time <= now]
    return len(chosen), sum(event.amount_minor for event in chosen)


def _merchant(events: list[TransactionEvent], now: datetime) -> tuple[int, float]:
    current = _minute(now)
    chosen = [event for event in events if current - 1_440 < _minute(event.event_time) <= current]
    count = len(chosen)
    if count == 0:
        return 0, 0.0
    declined = sum(event.status == "declined" for event in chosen)
    return count, declined / count


async def _post(
    client: httpx.AsyncClient,
    *,
    card_token: str,
    merchant_id: str,
    lat: float,
    lon: float,
) -> dict[str, object]:
    response = await client.post(
        "/v1/features",
        json={
            "card_token": card_token,
            "merchant_id": merchant_id,
            "ts": datetime.now(UTC).isoformat(),
            "lat": lat,
            "lon": lon,
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, dict)
    return body


def _features(body: dict[str, object]) -> dict[str, dict[str, object]]:
    raw = body["features"]
    assert isinstance(raw, dict)
    features: dict[str, dict[str, object]] = {}
    for name, value in raw.items():
        assert isinstance(name, str)
        assert isinstance(value, dict)
        features[name] = value
    return features


def _matches(
    body: dict[str, object],
    card_events: list[TransactionEvent],
    merchant_events: list[TransactionEvent],
    now: datetime,
) -> bool:
    features = _features(body)
    if set(features) != {spec.name for spec in CATALOG}:
        return False
    if any(item["status"] != FeatureStatus.fresh for item in features.values()):
        return False
    count_1h, sum_1h = _card_sum(card_events, now, _HOUR)
    count_24h, sum_24h = _card_sum(card_events, now, _DAY)
    merchant_count, decline = _merchant(merchant_events, now)
    return (
        features["card_cnt_1h"]["value"] == count_1h
        and features["card_sum_1h"]["value"] == sum_1h
        and features["card_cnt_24h"]["value"] == count_24h
        and features["card_sum_24h"]["value"] == sum_24h
        and features["merchant_cnt_24h"]["value"] == merchant_count
        and features["merchant_decline_rate_24h"]["value"] == pytest.approx(decline)
    )


async def test_served_features_follow_the_writer_heartbeat() -> None:
    stats = await run_sender(
        url=_INGESTION,
        tps=20,
        duration_s=5,
        seed=7,
        run_id=str(uuid4()),
        report=False,
    )
    assert stats.accepted > 0
    events = await _read_events()
    assert events
    counts: dict[str, int] = {}
    for event in events:
        counts[event.card_token] = counts.get(event.card_token, 0) + 1
    card_token = max(counts, key=lambda token: counts[token])
    card_events = [event for event in events if event.card_token == card_token]
    latest = max(card_events, key=lambda event: event.event_time)
    assert latest.lat is not None and latest.lon is not None
    merchant_id = latest.merchant_id
    merchant_events = [event for event in events if event.merchant_id == merchant_id]
    card_names = {spec.name for spec in for_entity("card") if spec.kind == "stream"}

    async with httpx.AsyncClient(base_url=_API, timeout=10) as client:
        deadline = time.monotonic() + 40
        matched: dict[str, object] | None = None
        while time.monotonic() < deadline:
            now = datetime.now(UTC)
            body = await _post(
                client,
                card_token=card_token,
                merchant_id=merchant_id,
                lat=latest.lat,
                lon=latest.lon,
            )
            if _matches(body, card_events, merchant_events, now):
                matched = body
                break
            await asyncio.sleep(0.5)
        assert matched is not None

        _compose("stop", "features-worker")
        try:
            await asyncio.sleep(6.5)
            stale = _features(
                await _post(
                    client,
                    card_token=card_token,
                    merchant_id=merchant_id,
                    lat=latest.lat,
                    lon=latest.lon,
                )
            )
            for name in card_names:
                assert stale[name]["status"] == FeatureStatus.stale
        finally:
            _compose("start", "features-worker")

        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            restored = _features(
                await _post(
                    client,
                    card_token=card_token,
                    merchant_id=merchant_id,
                    lat=latest.lat,
                    lon=latest.lon,
                )
            )
            if all(restored[name]["status"] == FeatureStatus.fresh for name in card_names):
                return
            await asyncio.sleep(0.5)
    pytest.fail("card features stayed stale after the worker restarted")


def _group_offsets(group: str) -> dict[int, int] | None:
    admin = KafkaAdminClient(bootstrap_servers=_BOOTSTRAP, client_id="baseline-e2e-offsets")
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
        if tp.topic == _TOPIC:
            found[int(tp.partition)] = int(meta.offset)
    return found


def _caught_up(committed: dict[int, int] | None, ends: dict[int, int]) -> bool:
    if committed is None:
        return False
    return all(committed.get(partition, 0) >= end for partition, end in ends.items())


async def _wait_until_both_writers_are_caught_up() -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=_BOOTSTRAP, enable_auto_commit=False)
    partitions = [TopicPartition(_TOPIC, index) for index in range(_PARTITIONS)]
    consumer.assign(partitions)
    await consumer.start()
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            ends = await consumer.end_offsets(partitions)
            end_map = {tp.partition: int(ends[tp]) for tp in partitions}
            online = await asyncio.to_thread(_group_offsets, "online-writer")
            baseline = await asyncio.to_thread(_group_offsets, "baseline-writer")
            if _caught_up(online, end_map) and _caught_up(baseline, end_map):
                return
            await asyncio.sleep(0.5)
    finally:
        await consumer.stop()
    pytest.fail(
        "online-writer and baseline-writer did not reach lag 0; is `make baseline-up` running?"
    )


def _pairs(events: list[TransactionEvent]) -> list[tuple[str, str, TransactionEvent]]:
    by_card: dict[str, list[TransactionEvent]] = {}
    by_merchant: dict[str, list[TransactionEvent]] = {}
    for event in events:
        by_card.setdefault(event.card_token, []).append(event)
        by_merchant.setdefault(event.merchant_id, []).append(event)
    cards = sorted(by_card, key=lambda token: len(by_card[token]), reverse=True)[:20]
    merchants = sorted(by_merchant, key=lambda mid: len(by_merchant[mid]), reverse=True)[:20]
    pairs: list[tuple[str, str, TransactionEvent]] = []
    seen: set[tuple[str, str]] = set()
    for token in cards:
        event = max(by_card[token], key=lambda item: item.event_time)
        key = (token, event.merchant_id)
        if key not in seen:
            seen.add(key)
            pairs.append((token, event.merchant_id, event))
    for merchant_id in merchants:
        event = max(by_merchant[merchant_id], key=lambda item: item.event_time)
        key = (event.card_token, merchant_id)
        if key not in seen:
            seen.add(key)
            pairs.append((event.card_token, merchant_id, event))
    return pairs


def _request_body(card: str, merchant: str, event: TransactionEvent) -> dict[str, object]:
    body: dict[str, object] = {
        "card_token": card,
        "merchant_id": merchant,
        "ts": datetime.now(UTC).isoformat(),
    }
    if event.lat is not None and event.lon is not None:
        body["lat"] = event.lat
        body["lon"] = event.lon
    return body


async def _post_body(client: httpx.AsyncClient, body: dict[str, object]) -> dict[str, object]:
    response = await client.post("/v1/features", json=body)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _stream_values_match(left: dict[str, object], right: dict[str, object]) -> bool:
    left_features = _features(left)
    right_features = _features(right)
    if "geo_velocity_kmh" not in left_features or "geo_velocity_kmh" not in right_features:
        return False
    for name in _STREAM:
        if name not in left_features or name not in right_features:
            return False
        online = left_features[name]["value"]
        baseline = right_features[name]["value"]
        if name == "merchant_decline_rate_24h":
            if online != pytest.approx(baseline):
                return False
        elif online != baseline:
            return False
    return True


async def test_baseline_stream_features_match_online() -> None:
    """One stream, both writers at lag 0: stream values match. Geo is not compared."""
    stats = await run_sender(
        url=_INGESTION,
        tps=20,
        duration_s=5,
        seed=11,
        run_id=str(uuid4()),
        report=False,
    )
    assert stats.accepted > 0
    await _wait_until_both_writers_are_caught_up()
    events = await _read_events()
    pairs = _pairs(events)
    assert pairs
    async with (
        httpx.AsyncClient(base_url=_API, timeout=10) as online,
        httpx.AsyncClient(base_url=_BASELINE, timeout=10) as baseline,
    ):
        for card, merchant, event in pairs:
            body = _request_body(card, merchant, event)
            matched = False
            last_online: dict[str, object] = {}
            last_baseline: dict[str, object] = {}
            for _ in range(3):
                last_online = await _post_body(online, body)
                last_baseline = await _post_body(baseline, body)
                if _stream_values_match(last_online, last_baseline):
                    matched = True
                    break
                await asyncio.sleep(0.3)
            assert matched, f"{card}/{merchant} diverged: {last_online} vs {last_baseline}"
