import json
import logging
from dataclasses import dataclass
from datetime import timedelta

import pytest
from fakeredis import FakeAsyncRedis
from prometheus_client import CollectorRegistry
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from support import BASE, make_event

from features.metrics import WorkerMetrics, worker_metrics
from features.settings import Settings
from features.windows import (
    merchant_features_key,
    minute_bucket,
    stream_specs,
    to_epoch_ms,
    watermark_key,
)
from features.worker import JsonFormatter, OnlineWriter, apply_event, load_script
from schemas import FeatureSnapshot

NOW = to_epoch_ms(BASE)
TOPIC = "transactions.raw"


class StopLoop(Exception):
    pass


@dataclass(frozen=True)
class Partition:
    topic: str
    partition: int


class Record:
    def __init__(
        self,
        value: bytes | None,
        *,
        partition: int = 0,
        offset: int = 0,
        topic: str = TOPIC,
    ) -> None:
        self.value = value
        self.partition = partition
        self.offset = offset
        self.topic = topic


class FakeConsumer:
    def __init__(
        self,
        *,
        assignment: set[Partition] | None = None,
        positions: dict[Partition, int] | None = None,
        ends: dict[Partition, int] | None = None,
    ) -> None:
        self._assignment = assignment or set()
        self.positions = positions or {}
        self.ends = ends or {}
        self.commits: list[dict[object, int]] = []
        self.commit_failures = 0

    def assignment(self) -> set[Partition]:
        return self._assignment

    async def position(self, tp: Partition) -> int:
        return self.positions[tp]

    async def end_offsets(self, partitions: list[Partition]) -> dict[Partition, int]:
        return {tp: self.ends[tp] for tp in partitions}

    async def commit(self, offsets: dict[object, int]) -> None:
        if self.commit_failures:
            self.commit_failures -= 1
            raise ConnectionError("commit down")
        self.commits.append(offsets)


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.failures = 0

    async def send_and_wait(self, topic: str, *, value: bytes, key: bytes) -> None:
        if self.failures:
            self.failures -= 1
            raise OSError("broker down")
        self.sent.append((topic, value, key))


class FlakyRedis:
    def __init__(self, inner: FakeAsyncRedis, failures: int, noscript: bool = False) -> None:
        self.inner = inner
        self.failures = failures
        self.noscript = noscript
        self.calls = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self.inner, name)

    async def evalsha(self, *args: object, **kwargs: object) -> object:
        self.calls += 1
        if self.failures:
            self.failures -= 1
            if self.noscript:
                raise ResponseError("NOSCRIPT No matching script")
            raise RedisConnectionError("down")
        return await self.inner.evalsha(*args, **kwargs)


async def _noop_sleep(_seconds: float) -> None:
    return None


async def _stop_sleep(_seconds: float) -> None:
    raise StopLoop


def _writer(
    redis: object,
    consumer: FakeConsumer | None = None,
    producer: FakeProducer | None = None,
    *,
    sleep: object = _noop_sleep,
    metrics: WorkerMetrics | None = None,
) -> OnlineWriter:
    return OnlineWriter(
        Settings(),
        redis,  # type: ignore[arg-type]
        consumer or FakeConsumer(),  # type: ignore[arg-type]
        producer or FakeProducer(),  # type: ignore[arg-type]
        now_ms=lambda: NOW,
        sleep=sleep,  # type: ignore[arg-type]
        metrics=metrics,
    )


def test_heartbeat_interval_above_two_seconds_is_rejected() -> None:
    with pytest.raises(ValueError, match="heartbeat_interval_s"):
        Settings(heartbeat_interval_s=2.1)


def test_from_env_reads_broker_redis_and_heartbeat(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/1")
    monkeypatch.setenv("HEARTBEAT_INTERVAL_S", "2")
    settings = Settings.from_env()
    assert settings.kafka_bootstrap == "kafka:9092"
    assert settings.redis_url == "redis://redis:6379/1"
    assert settings.heartbeat_interval_s == 2.0
    assert settings.group_id == "online-writer"
    assert settings.raw_topic == "transactions.raw"
    assert settings.updates_topic == "features.updates"


def test_json_log_includes_event_fields() -> None:
    record = logging.LogRecord(
        name="features",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="applied event",
        args=(),
        exc_info=None,
    )
    record.event_id = "evt-1"  # type: ignore[attr-defined]
    record.partition = 2  # type: ignore[attr-defined]
    record.offset = 9  # type: ignore[attr-defined]
    record.outcome = "applied"  # type: ignore[attr-defined]
    record.applied = 1  # type: ignore[attr-defined]
    record.dup = 0  # type: ignore[attr-defined]
    record.skipped = 0  # type: ignore[attr-defined]

    payload = json.loads(JsonFormatter().format(record))

    assert payload["event_id"] == "evt-1"
    assert payload["partition"] == 2
    assert payload["offset"] == 9
    assert payload["outcome"] == "applied"
    assert payload["applied"] == 1


async def test_handle_publishes_card_and_merchant_snapshots() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    producer = FakeProducer()
    consumer = FakeConsumer()
    writer = _writer(redis, consumer, producer)
    event = make_event(amount_minor=180, status="declined")

    await writer.handle(Record(event.to_bytes(), partition=1, offset=3))  # type: ignore[arg-type]

    assert writer.counters.applied == 1
    from aiokafka.structs import TopicPartition

    assert consumer.commits == [{TopicPartition(TOPIC, 1): 4}]
    assert len(producer.sent) == 2
    card = FeatureSnapshot.from_bytes(producer.sent[0][1])
    merchant = FeatureSnapshot.from_bytes(producer.sent[1][1])
    assert card.entity_type == "card"
    assert set(card.features) == {spec.name for spec in stream_specs("card")}
    assert "geo_velocity_kmh" not in card.features
    assert card.features["card_cnt_1m"] == 1
    assert card.features["card_sum_1m"] == 180
    assert card.source_partition == 1
    assert card.source_offset == 3
    assert producer.sent[0][2] == event.card_token.encode()
    assert merchant.entity_type == "merchant"
    assert set(merchant.features) == {spec.name for spec in stream_specs("merchant")}
    assert merchant.features["merchant_cnt_24h"] == 1
    assert merchant.features["merchant_decline_rate_24h"] == 1.0
    assert producer.sent[1][2] == event.merchant_id.encode()


async def test_poison_payload_is_skipped_and_committed(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO, logger="features")
    redis = FakeAsyncRedis(decode_responses=True)
    consumer = FakeConsumer()
    producer = FakeProducer()
    writer = _writer(redis, consumer, producer)
    body = b'{"event_id": "evt-9", "amount_minor": -1}'

    await writer.handle(Record(body, partition=2, offset=8))  # type: ignore[arg-type]

    assert writer.counters.skipped == 1
    assert producer.sent == []
    from aiokafka.structs import TopicPartition

    assert consumer.commits == [{TopicPartition(TOPIC, 2): 9}]
    assert any(getattr(record, "event_id", None) == "evt-9" for record in caplog.records)
    assert await redis.dbsize() == 0


async def test_redis_outage_does_not_commit() -> None:
    inner = FakeAsyncRedis(decode_responses=True)
    redis = FlakyRedis(inner, failures=3)
    consumer = FakeConsumer()
    writer = _writer(redis, consumer, sleep=_stop_sleep)

    with pytest.raises(StopLoop):
        await writer.handle(Record(make_event().to_bytes()))  # type: ignore[arg-type]

    assert consumer.commits == []
    assert redis.calls >= 1
    assert writer.counters.applied == 0


async def test_redis_outage_retries_then_applies_once() -> None:
    inner = FakeAsyncRedis(decode_responses=True)
    redis = FlakyRedis(inner, failures=1)
    consumer = FakeConsumer()
    writer = _writer(redis, consumer)

    await writer.handle(Record(make_event().to_bytes()))  # type: ignore[arg-type]

    # fail + successful apply + merchant_read.lua during snapshot publish
    assert redis.calls == 3
    assert len(consumer.commits) == 1
    assert writer.counters.applied == 1
    assert writer.counters.dup == 0


async def test_publish_failure_retries_without_double_counting() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    producer = FakeProducer()
    producer.failures = 1
    consumer = FakeConsumer()
    writer = _writer(redis, consumer, producer)

    await writer.handle(Record(make_event().to_bytes()))  # type: ignore[arg-type]

    assert writer.counters.applied == 1
    assert writer.counters.dup == 0
    assert len(producer.sent) == 2
    assert len(consumer.commits) == 1


async def test_equal_watermark_republishes_without_changing_buckets() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    event = make_event()
    sha = await load_script(redis)
    assert await apply_event(redis, sha, event, partition=0, offset=7, now_ms=NOW) == "applied"
    producer = FakeProducer()
    consumer = FakeConsumer()
    writer = _writer(redis, consumer, producer)

    await writer.handle(Record(event.to_bytes(), offset=7))  # type: ignore[arg-type]

    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(NOW)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert writer.counters.dup == 1
    assert len(producer.sent) == 2
    assert len(consumer.commits) == 1


async def test_older_offset_is_committed_without_republish() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    event = make_event()
    sha = await load_script(redis)
    await apply_event(redis, sha, event, partition=0, offset=7, now_ms=NOW)
    producer = FakeProducer()
    consumer = FakeConsumer()
    writer = _writer(redis, consumer, producer)

    await writer.handle(Record(make_event().to_bytes(), offset=3))  # type: ignore[arg-type]

    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(NOW)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert producer.sent == []
    assert writer.counters.dup == 1
    from aiokafka.structs import TopicPartition

    assert consumer.commits == [{TopicPartition(TOPIC, 0): 4}]


async def test_dup_event_advances_watermark_without_publish() -> None:
    registry = CollectorRegistry()
    redis = FakeAsyncRedis(decode_responses=True)
    event = make_event(amount_minor=180, status="declined")
    sha = await load_script(redis)
    assert await apply_event(redis, sha, event, partition=0, offset=7, now_ms=NOW) == "applied"
    producer = FakeProducer()
    consumer = FakeConsumer()
    writer = _writer(redis, consumer, producer, metrics=worker_metrics(registry))

    await writer.handle(Record(event.to_bytes(), offset=8))  # type: ignore[arg-type]

    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(NOW)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert await redis.hget(feat, f"m:{minute}:d") == "1"
    assert await redis.hget(watermark_key(0), "offset") == "8"
    assert producer.sent == []
    assert writer.counters.dup_event == 1
    assert writer.counters.dup == 0
    assert registry.get_sample_value("features_events_total", {"outcome": "dup_event"}) == 1.0
    from aiokafka.structs import TopicPartition

    assert consumer.commits == [{TopicPartition(TOPIC, 0): 9}]


async def test_noscript_reloads_and_applies() -> None:
    inner = FakeAsyncRedis(decode_responses=True)
    redis = FlakyRedis(inner, failures=1, noscript=True)
    consumer = FakeConsumer()
    writer = _writer(redis, consumer)

    await writer.handle(Record(make_event().to_bytes()))  # type: ignore[arg-type]

    assert writer.counters.applied == 1
    assert len(consumer.commits) == 1


async def test_commit_failure_is_retried() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    consumer = FakeConsumer()
    consumer.commit_failures = 1
    writer = _writer(redis, consumer)

    await writer.handle(Record(make_event().to_bytes(), offset=4))  # type: ignore[arg-type]

    assert len(consumer.commits) == 1
    assert writer.counters.applied == 1


async def test_heartbeat_refreshes_only_caught_up_partitions() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    caught = Partition(TOPIC, 0)
    behind = Partition(TOPIC, 1)
    other = Partition("other", 2)
    await redis.hset(watermark_key(0), mapping={"offset": "4", "as_of_ms": "10"})
    await redis.hset(watermark_key(1), mapping={"offset": "1", "as_of_ms": "10"})
    consumer = FakeConsumer(
        assignment={caught, behind, other},
        positions={caught: 5, behind: 1, other: 9},
        ends={caught: 5, behind: 8, other: 9},
    )
    writer = _writer(redis, consumer)

    await writer.heartbeat_once()

    assert await redis.hget(watermark_key(0), "as_of_ms") == str(NOW)
    assert await redis.hget(watermark_key(0), "offset") == "4"
    assert await redis.hget(watermark_key(1), "as_of_ms") == "10"
    assert await redis.hget(watermark_key(2), "as_of_ms") is None


async def test_heartbeat_marks_an_empty_log() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    tp = Partition(TOPIC, 3)
    consumer = FakeConsumer(assignment={tp}, positions={tp: 0}, ends={tp: 0})
    writer = _writer(redis, consumer)

    await writer.heartbeat_once()

    assert await redis.hget(watermark_key(3), "as_of_ms") == str(NOW)
    assert await redis.hget(watermark_key(3), "offset") is None


async def test_applied_latency_is_bucketed_and_a_duplicate_is_not() -> None:
    registry = CollectorRegistry()
    redis = FakeAsyncRedis(decode_responses=True)
    writer = _writer(redis, metrics=worker_metrics(registry))
    event = make_event(ingest_time=BASE - timedelta(milliseconds=300))

    await writer.handle(Record(event.to_bytes(), offset=4))  # type: ignore[arg-type]

    assert (
        registry.get_sample_value(
            "features_event_to_online_seconds_bucket",
            {"le": "0.5"},
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "features_event_to_online_seconds_bucket",
            {"le": "0.25"},
        )
        == 0
    )
    assert registry.get_sample_value("features_event_to_online_seconds_count") == 1

    await writer.handle(Record(event.to_bytes(), offset=4))  # type: ignore[arg-type]

    assert writer.counters.dup == 1
    assert registry.get_sample_value("features_event_to_online_seconds_count") == 1
