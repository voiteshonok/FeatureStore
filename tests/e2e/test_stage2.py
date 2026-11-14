"""Stage 2 acceptance: simulator, ingestion, and Kafka on the compose stack.

Requires ``make secret`` and ``make up``. ``transactions.raw`` must be empty.
A second run needs ``make clean`` first: the seeded stream repeats event ids.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections import Counter
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError, UnknownTopicOrPartitionError
from aiokafka.structs import ConsumerRecord

from ingestion.tokenize import load_key, tokenize
from pan_scan.scan import main as scan_for_pans
from schemas import TransactionEvent
from simulator.config import load_config
from simulator.sender import run_sender

pytestmark = pytest.mark.e2e

_TPS = 50
_DURATION_S = 60
_SEED = 21
_INGESTION_URL = "http://localhost:8000"
_KAFKA_BOOTSTRAP = "localhost:29092"
_TOPIC = "transactions.raw"
_P95_LIMIT_MS = 1000
_MIN_ACCEPTED = 1000
_READ_TIMEOUT_S = 30.0
_KEY_PATH = Path(__file__).resolve().parents[2] / "secrets" / "hmac_key"


@dataclass(frozen=True)
class _Posted:
    """One simulator response, including the body that carried the PAN."""

    body: dict[str, object]
    status_code: int
    duplicate: bool | None


def _require_profile() -> None:
    config = load_config()
    assert config.duplicate_rate > 0
    assert config.late_short_rate > 0
    assert config.late_long_rate > 0
    assert config.retry_rate > 0


def _text(body: dict[str, object], name: str) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value:
        pytest.fail(f"posted event is missing {name}")
    return value


def _percentile(values: list[int], percentile: float) -> int:
    """Nearest-rank percentile, matching the simulator's latency summary."""
    ordered = sorted(values)
    rank = math.ceil(percentile / 100 * len(ordered)) - 1
    return ordered[min(max(rank, 0), len(ordered) - 1)]


def _json_object(value: object, what: str) -> dict[str, object]:
    if not isinstance(value, dict):
        msg = f"{what} is not a JSON object"
        raise TypeError(msg)
    return {str(key): item for key, item in value.items()}


@asynccontextmanager
async def _consumer() -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        _TOPIC,
        bootstrap_servers=_KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        try:
            await consumer.start()
        except UnknownTopicOrPartitionError:
            pytest.fail(f"topic {_TOPIC} not found; run make topics")
        except (OSError, KafkaError) as exc:
            pytest.fail(f"kafka at {_KAFKA_BOOTSTRAP} failed ({exc}); run make up")
        yield consumer
    finally:
        await consumer.stop()


def _partitions(consumer: AIOKafkaConsumer) -> list[TopicPartition]:
    assigned = sorted(
        (tp for tp in consumer.assignment() if tp.topic == _TOPIC),
        key=lambda tp: tp.partition,
    )
    if not assigned:
        pytest.fail(f"topic {_TOPIC} not found; run make topics")
    return assigned


async def _require_ready() -> None:
    if not _KEY_PATH.is_file():
        pytest.fail(f"HMAC key missing: {_KEY_PATH}; run make secret")
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{_INGESTION_URL}/ready")
    except httpx.HTTPError as exc:
        pytest.fail(f"ingestion is not up ({exc}); run make secret && make up")
    if response.status_code != 200:
        pytest.fail(f"ingestion /ready returned {response.status_code}; run make secret && make up")


async def _require_empty_topic() -> None:
    async with _consumer() as consumer:
        end_offsets = await consumer.end_offsets(_partitions(consumer))
    if any(offset != 0 for offset in end_offsets.values()):
        pytest.fail(
            f"{_TOPIC} is not empty; run make clean && make secret && make up before test-e2e"
        )


async def _read_topic() -> list[ConsumerRecord[bytes, bytes]]:
    async with _consumer() as consumer:
        assigned = _partitions(consumer)
        end_offsets = await consumer.end_offsets(assigned)
        expected = sum(end_offsets.values())
        reset = await consumer.seek_to_beginning(*assigned)
        if not reset:
            pytest.fail(f"could not seek to the beginning of {_TOPIC}")
        found: list[ConsumerRecord[bytes, bytes]] = []
        deadline = time.monotonic() + _READ_TIMEOUT_S
        while len(found) < expected and time.monotonic() < deadline:
            batches = await consumer.getmany(timeout_ms=1000, max_records=500)
            for messages in batches.values():
                found.extend(messages)
        if len(found) != expected:
            pytest.fail(f"read {len(found)} of {expected} records from {_TOPIC}")
        return found


async def _record(posts: list[_Posted], response: httpx.Response) -> None:
    await response.aread()
    request_body = response.request.content
    if not isinstance(request_body, bytes):
        msg = "simulator request body was not available"
        raise TypeError(msg)
    body = _json_object(json.loads(request_body), "simulator request")
    duplicate: bool | None = None
    if response.status_code == 202:
        accepted = _json_object(response.json(), "ingestion response")
        duplicate = bool(accepted.get("duplicate"))
    posts.append(_Posted(body, response.status_code, duplicate))


async def test_stage2() -> None:
    """Accepted events land once, tokenized, and within a second of ingest."""
    await _require_ready()
    await _require_empty_topic()
    _require_profile()

    posts: list[_Posted] = []

    async def record(response: httpx.Response) -> None:
        await _record(posts, response)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=5.0),
        limits=httpx.Limits(max_connections=2000, max_keepalive_connections=200),
        event_hooks={"response": [record]},
    ) as client:
        stats = await run_sender(
            url=_INGESTION_URL,
            tps=_TPS,
            duration_s=_DURATION_S,
            seed=_SEED,
            client=client,
            report=False,
        )

    assert stats.failed == 0
    assert stats.http_503 == 0
    assert stats.http_422 == 0
    assert stats.duplicates > 0
    assert stats.accepted >= _MIN_ACCEPTED
    assert stats.accepted + stats.duplicates == stats.generated

    accepted = [post for post in posts if post.status_code == 202 and post.duplicate is False]
    assert len(accepted) == stats.accepted
    pan_by_event = {_text(post.body, "event_id"): _text(post.body, "pan") for post in accepted}
    assert len(pan_by_event) == len(accepted)

    # US-1.1: the log contains each accepted event and nothing else.
    records = await _read_topic()
    assert len(records) == stats.accepted

    # US-1.2: the same card always maps to the same token, and the PAN stays out.
    key = load_key(_KEY_PATH)
    events: list[TransactionEvent] = []
    delays_ms: list[int] = []
    tokens_by_pan: dict[str, set[str]] = {}
    uses_by_pan: dict[str, int] = {}
    for record in records:
        value = record.value
        assert isinstance(value, bytes)
        raw = json.loads(value)
        assert isinstance(raw, dict)
        if "pan" in raw:
            pytest.fail("kafka record contains a pan field")
        event = TransactionEvent.from_bytes(value)
        if not event.card_token:
            pytest.fail(f"event {event.event_id} has an empty card token")
        event_id = str(event.event_id)
        if event_id not in pan_by_event:
            pytest.fail(f"kafka event {event_id} was not accepted by the simulator")
        pan = pan_by_event[event_id]
        expected = tokenize(pan, key)
        if event.card_token != expected:
            pytest.fail(f"event {event.event_id} token does not match the card")
        tokens_by_pan.setdefault(pan, set()).add(event.card_token)
        uses_by_pan[pan] = uses_by_pan.get(pan, 0) + 1
        events.append(event)
        ingest_ms = round(event.ingest_time.timestamp() * 1000)
        delays_ms.append(record.timestamp - ingest_ms)

    for tokens in tokens_by_pan.values():
        if len(tokens) != 1:
            pytest.fail("one card mapped to more than one token")
    distinct_tokens = {next(iter(tokens)) for tokens in tokens_by_pan.values()}
    if len(distinct_tokens) != len(tokens_by_pan):
        pytest.fail("different cards mapped to the same token")
    if not any(count > 1 for count in uses_by_pan.values()):
        pytest.fail("no card was accepted more than once")
    assert await asyncio.to_thread(scan_for_pans) == 0

    # US-1.3: one message per event id; a retry keeps the payment id.
    kafka_ids = [str(event.event_id) for event in events]
    assert len(kafka_ids) == len(set(kafka_ids))
    posted_counts = Counter(_text(post.body, "event_id") for post in posts)
    duplicate_ids = {event_id for event_id, count in posted_counts.items() if count > 1}
    assert duplicate_ids
    kafka_counts = Counter(kafka_ids)
    for event_id in duplicate_ids:
        assert kafka_counts[event_id] == 1

    payment_ids: dict[str, set[str]] = {}
    for post in accepted:
        payment_ids.setdefault(_text(post.body, "payment_id"), set()).add(
            _text(post.body, "event_id")
        )
    retry_groups = [event_ids for event_ids in payment_ids.values() if len(event_ids) > 1]
    assert retry_groups
    kafka_id_set = set(kafka_ids)
    for event_ids in retry_groups:
        assert len(event_ids) > 1
        assert event_ids <= kafka_id_set

    assert delays_ms
    assert all(delay > -_P95_LIMIT_MS for delay in delays_ms)
    assert _percentile(delays_ms, 95) <= _P95_LIMIT_MS
    assert any(event.ingest_time - event.event_time >= timedelta(seconds=1) for event in events)
