from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.partitioner import DefaultPartitioner
from aiokafka.structs import ConsumerRecord
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from ingestion import AcceptResult, IdempotencyStore, KafkaPublisher, PublishError
from ingestion.publisher import TOPIC
from schemas import TransactionEvent

pytestmark = pytest.mark.integration

KAFKA_IMAGE = "apache/kafka:3.9.0"
POSTGRES_IMAGE = "postgres:17"
INIT_SQL = Path(__file__).resolve().parents[3] / "infra" / "postgres" / "init.sql"
_PARTITIONS = [0, 1, 2, 3]
_CARD_A = "card-0"
_CARD_B = "card-1"


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _kafka_container(host_port: int) -> DockerContainer:
    kafka = DockerContainer(KAFKA_IMAGE)
    kafka.with_bind_ports(9092, host_port)
    kafka.with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
    kafka.with_env("KAFKA_NODE_ID", "1")
    kafka.with_env("KAFKA_PROCESS_ROLES", "broker,controller")
    kafka.with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
    kafka.with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{host_port}")
    kafka.with_env(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
    )
    kafka.with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
    kafka.with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
    kafka.with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
    kafka.with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    kafka.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    kafka.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    kafka.with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    kafka.with_env("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
    kafka.waiting_for(
        LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(timedelta(seconds=60))
    )
    return kafka


def _ensure_topic(bootstrap: str) -> None:
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="ingestion-tests",
        request_timeout_ms=5_000,
    )
    try:
        if TOPIC in admin.list_topics():
            return
        try:
            admin.create_topics(
                [NewTopic(name=TOPIC, num_partitions=len(_PARTITIONS), replication_factor=1)],
                timeout_ms=10_000,
                wait_for_metadata=True,
            )
        except TopicAlreadyExistsError:
            return
    finally:
        admin.close()


def _wait_for_kafka(bootstrap: str, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            _ensure_topic(bootstrap)
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)
        else:
            return
    msg = f"Kafka did not become ready at {bootstrap}"
    raise TimeoutError(msg) from last_error


@dataclass
class KafkaService:
    container: DockerContainer
    bootstrap: str

    def stop(self) -> None:
        """Stop the broker without deleting the container."""
        self.container.get_wrapped_container().stop(timeout=10)

    def start(self) -> None:
        self.container.get_wrapped_container().start()


@pytest.fixture(scope="module")
def kafka() -> Iterator[KafkaService]:
    host_port = _unused_port()
    bootstrap = f"127.0.0.1:{host_port}"
    container = _kafka_container(host_port)
    with container:
        _wait_for_kafka(bootstrap)
        yield KafkaService(container, bootstrap)


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as postgres:
        dsn = psycopg.conninfo.make_conninfo(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(postgres.port),
            user=postgres.username,
            password=postgres.password,
            dbname=postgres.dbname,
        )
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(INIT_SQL.read_text())
            conn.commit()
        yield dsn


@pytest.fixture
async def pool(
    postgres_dsn: str,
) -> AsyncIterator[AsyncConnectionPool[AsyncConnection[TupleRow]]]:
    connection_pool: AsyncConnectionPool[AsyncConnection[TupleRow]] = AsyncConnectionPool(
        postgres_dsn,
        min_size=2,
        max_size=4,
        open=False,
    )
    await connection_pool.open(wait=True, timeout=30)
    try:
        async with connection_pool.connection() as conn, conn.cursor() as cur:
            await cur.execute("TRUNCATE ingestion.idempotency_keys")
        yield connection_pool
    finally:
        await connection_pool.close()


@pytest.fixture
def store(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
) -> IdempotencyStore:
    return IdempotencyStore(pool)


@pytest.fixture
async def publisher(kafka: KafkaService) -> AsyncIterator[KafkaPublisher]:
    kafka_publisher = KafkaPublisher(kafka.bootstrap)
    await kafka_publisher.start()
    try:
        yield kafka_publisher
    finally:
        await kafka_publisher.aclose()


def _event(*, event_id: UUID | None = None, card_token: str = _CARD_A) -> TransactionEvent:
    now = datetime.now(UTC).replace(microsecond=0)
    return TransactionEvent(
        event_id=event_id or uuid4(),
        payment_id="pay-1",
        merchant_id="m-1",
        amount_minor=100,
        currency="EUR",
        status="approved",
        event_time=now,
        card_token=card_token,
        ingest_time=now,
    )


def _partition(key: bytes) -> int:
    return int(DefaultPartitioner()(key, _PARTITIONS, list(_PARTITIONS)))


async def _has_key(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    event_id: UUID,
) -> bool:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM ingestion.idempotency_keys WHERE event_id = %s",
                (event_id,),
            )
            row = await cur.fetchone()
    return row is not None


async def _records_for(
    bootstrap: str,
    event_ids: set[UUID],
    *,
    wait_s: float = 20,
) -> list[ConsumerRecord]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    partitions = [TopicPartition(TOPIC, index) for index in _PARTITIONS]
    consumer.assign(partitions)
    await consumer.start()
    try:
        await consumer.seek_to_beginning(*partitions)
        found: list[ConsumerRecord] = []
        deadline = time.monotonic() + wait_s
        while time.monotonic() < deadline and len(found) < len(event_ids):
            await _collect(consumer, event_ids, found)
        extra_deadline = time.monotonic() + 1
        while time.monotonic() < extra_deadline:
            await _collect(consumer, event_ids, found)
        return found
    finally:
        await consumer.stop()


async def _collect(
    consumer: AIOKafkaConsumer,
    event_ids: set[UUID],
    found: list[ConsumerRecord],
) -> None:
    batches = await consumer.getmany(timeout_ms=400, max_records=100)
    for messages in batches.values():
        for message in messages:
            event = TransactionEvent.from_bytes(message.value)
            if event.event_id in event_ids:
                found.append(message)


async def test_new_event_is_published_once_and_stored(
    store: IdempotencyStore,
    publisher: KafkaPublisher,
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    kafka: KafkaService,
) -> None:
    event = _event()
    assert await store.accept(event, publisher.publish) is AcceptResult.ACCEPTED
    assert await _has_key(pool, event.event_id)
    records = await _records_for(kafka.bootstrap, {event.event_id})
    assert len(records) == 1
    assert records[0].key == event.card_token.encode("utf-8")
    assert TransactionEvent.from_bytes(records[0].value) == event


async def test_duplicate_event_id_publishes_one_message(
    store: IdempotencyStore,
    publisher: KafkaPublisher,
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    kafka: KafkaService,
) -> None:
    event = _event()
    assert await store.accept(event, publisher.publish) is AcceptResult.ACCEPTED
    assert await store.accept(event, publisher.publish) is AcceptResult.DUPLICATE
    assert await _has_key(pool, event.event_id)
    records = await _records_for(kafka.bootstrap, {event.event_id})
    assert len(records) == 1


async def test_concurrent_accept_publishes_once(
    store: IdempotencyStore,
    publisher: KafkaPublisher,
    kafka: KafkaService,
) -> None:
    event = _event()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def publish(item: TransactionEvent) -> None:
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        await publisher.publish(item)

    first = asyncio.create_task(store.accept(event, publish))
    second: asyncio.Task[AcceptResult] | None = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=10)
        second = asyncio.create_task(store.accept(event, publish))
        await asyncio.sleep(0.2)
        assert calls == 1
        release.set()
        results = await asyncio.wait_for(asyncio.gather(first, second), timeout=15)
    finally:
        release.set()
        for task in (first, second):
            if task is not None and not task.done():
                task.cancel()
    assert sorted(result.value for result in results) == ["accepted", "duplicate"]
    assert calls == 1
    records = await _records_for(kafka.bootstrap, {event.event_id})
    assert len(records) == 1


async def test_kafka_stopped_rolls_back_and_retry_publishes(
    store: IdempotencyStore,
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    kafka: KafkaService,
) -> None:
    publisher = KafkaPublisher(kafka.bootstrap, ack_timeout_s=3)
    await publisher.start()
    event = _event()
    await asyncio.to_thread(kafka.stop)
    try:
        with pytest.raises(PublishError):
            await store.accept(event, publisher.publish)
        assert not await _has_key(pool, event.event_id)
    finally:
        await publisher.aclose()
        await asyncio.sleep(0.2)
        await asyncio.to_thread(kafka.start)
        await asyncio.to_thread(_wait_for_kafka, kafka.bootstrap)

    retry = KafkaPublisher(kafka.bootstrap)
    await retry.start()
    try:
        assert await store.accept(event, retry.publish) is AcceptResult.ACCEPTED
        records = await _records_for(kafka.bootstrap, {event.event_id})
        assert len(records) == 1
        assert records[0].key == event.card_token.encode("utf-8")
        assert TransactionEvent.from_bytes(records[0].value) == event
    finally:
        await retry.aclose()


async def test_message_key_sticks_to_one_partition(
    store: IdempotencyStore,
    publisher: KafkaPublisher,
    kafka: KafkaService,
) -> None:
    assert _partition(_CARD_A.encode()) != _partition(_CARD_B.encode())
    first = _event(card_token=_CARD_A)
    second = _event(card_token=_CARD_A)
    other = _event(card_token=_CARD_B)
    for event in (first, second, other):
        assert await store.accept(event, publisher.publish) is AcceptResult.ACCEPTED
    records = await _records_for(
        kafka.bootstrap,
        {first.event_id, second.event_id, other.event_id},
    )
    assert len(records) == 3
    partitions_by_key: dict[bytes, set[int]] = {}
    for record in records:
        assert record.key is not None
        partitions_by_key.setdefault(record.key, set()).add(record.partition)
        assert record.partition == _partition(record.key)
    assert partitions_by_key[_CARD_A.encode()] == {_partition(_CARD_A.encode())}
    assert len(partitions_by_key[_CARD_A.encode()]) == 1
