"""Feature API freshness on the compose stack.

Runs after the stage 2 burst: ``transactions.raw`` may already hold events.
Requires ``make secret`` and ``make up``.
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

from features.windows import to_epoch_ms
from schemas import CATALOG, FeatureStatus, TransactionEvent, for_entity
from simulator.sender import run_sender

pytestmark = pytest.mark.e2e

_ROOT = Path(__file__).resolve().parents[2]
_COMPOSE = ["docker", "compose", "-f", str(_ROOT / "infra" / "docker-compose.yml")]
_API = "http://localhost:8001"
_INGESTION = "http://localhost:8000"
_BOOTSTRAP = "localhost:29092"
_TOPIC = "transactions.raw"
_PARTITIONS = 4
_HOUR = timedelta(hours=1)
_DAY = timedelta(hours=24)


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
