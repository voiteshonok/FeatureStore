from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord
from containers import PARTITIONS, KafkaService, wait_for_kafka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from ingestion.app import (
    Settings,
    _cleanup_loop,
    create_app,
)
from ingestion.idempotency import AcceptResult
from ingestion.publisher import TOPIC, PublishError
from schemas import TransactionEvent
from schemas.pan import is_luhn

VALID_PAN = "4111111111111111"
BAD_PAN = "4111111111111112"


def _body(**overrides: object) -> dict[str, object]:
    now = datetime.now(UTC).replace(microsecond=0)
    body: dict[str, object] = {
        "event_id": str(uuid4()),
        "payment_id": "pay-1",
        "pan": VALID_PAN,
        "merchant_id": "m-1",
        "amount_minor": 100,
        "currency": "EUR",
        "status": "approved",
        "event_time": now.isoformat(),
    }
    body.update(overrides)
    return body


class _Publisher:
    def __init__(self, *, ready: bool = True) -> None:
        self._ready = ready
        self.published: list[TransactionEvent] = []

    async def ready(self) -> bool:
        return self._ready

    async def publish(self, event: TransactionEvent) -> None:
        self.published.append(event)


class _Store:
    def __init__(self, result: AcceptResult | PublishError) -> None:
        self._result = result

    async def accept(
        self,
        event: TransactionEvent,
        publish: object,
    ) -> AcceptResult:
        if isinstance(self._result, PublishError):
            raise self._result
        if self._result is AcceptResult.ACCEPTED:
            await publish(event)  # type: ignore[operator]
        return self._result


class _Connection:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.queries: list[str] = []

    def connection(self) -> _Connection:
        return self

    async def execute(self, query: str) -> None:
        if self.fail:
            raise RuntimeError("database unavailable")
        self.queries.append(query)

    async def __aenter__(self) -> _Connection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


def _application(
    store: _Store,
    publisher: _Publisher,
    pool: _Connection | None = None,
) -> FastAPI:
    app = create_app(Settings(cleanup_interval_s=3600))
    app.state.store = store
    app.state.publisher = publisher
    app.state.pool = pool or _Connection()
    app.state.hmac_key = bytes(range(32))
    return app


@asynccontextmanager
async def _client(app: FastAPI) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


def _locs(body: dict[str, object]) -> list[object]:
    detail = body["detail"]
    assert isinstance(detail, list)
    locs: list[object] = []
    for item in detail:
        assert isinstance(item, dict)
        locs.append(item["loc"])
        assert set(item) == {"loc", "msg", "type"}
    return locs


async def test_health_does_not_check_dependencies() -> None:
    async with _client(create_app(Settings())) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_invalid_schema_names_the_field() -> None:
    app = _application(_Store(AcceptResult.ACCEPTED), _Publisher())
    async with _client(app) as client:
        response = await client.post("/v1/events", json=_body(currency="USD"))
    assert response.status_code == 422
    assert ["body", "currency"] in _locs(response.json())


async def test_bad_pan_is_not_echoed() -> None:
    assert not is_luhn(BAD_PAN)
    app = _application(_Store(AcceptResult.ACCEPTED), _Publisher())
    async with _client(app) as client:
        response = await client.post("/v1/events", json=_body(pan=BAD_PAN))
    assert response.status_code == 422
    assert BAD_PAN not in response.text
    assert ["body", "pan"] in _locs(response.json())


async def test_event_time_outside_the_window_names_the_field() -> None:
    old = (datetime.now(UTC) - timedelta(hours=25)).isoformat()
    app = _application(_Store(AcceptResult.ACCEPTED), _Publisher())
    async with _client(app) as client:
        response = await client.post("/v1/events", json=_body(event_time=old))
    assert response.status_code == 422
    assert ["body", "event_time"] in _locs(response.json())


async def test_accepted_event_returns_202() -> None:
    publisher = _Publisher()
    app = _application(_Store(AcceptResult.ACCEPTED), publisher)
    body = _body()
    async with _client(app) as client:
        response = await client.post("/v1/events", json=body)
    assert response.status_code == 202
    payload = response.json()
    assert payload == {"event_id": body["event_id"], "duplicate": False}
    assert len(publisher.published) == 1
    encoded = publisher.published[0].to_bytes().decode()
    assert VALID_PAN not in encoded
    assert publisher.published[0].card_token


async def test_duplicate_event_returns_202_without_publishing() -> None:
    publisher = _Publisher()
    app = _application(_Store(AcceptResult.DUPLICATE), publisher)
    async with _client(app) as client:
        response = await client.post("/v1/events", json=_body())
    assert response.status_code == 202
    assert response.json()["duplicate"] is True
    assert publisher.published == []


async def test_publish_error_returns_503_with_retry_after() -> None:
    app = _application(_Store(PublishError("down")), _Publisher())
    async with _client(app) as client:
        response = await client.post("/v1/events", json=_body())
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert VALID_PAN not in response.text


async def test_ready_reflects_postgres_and_kafka() -> None:
    healthy = _application(_Store(AcceptResult.ACCEPTED), _Publisher(), _Connection())
    async with _client(healthy) as client:
        response = await client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}

    database_down = _application(
        _Store(AcceptResult.ACCEPTED),
        _Publisher(),
        _Connection(fail=True),
    )
    async with _client(database_down) as client:
        response = await client.get("/ready")
    assert response.status_code == 503

    kafka_down = _application(_Store(AcceptResult.ACCEPTED), _Publisher(ready=False))
    async with _client(kafka_down) as client:
        response = await client.get("/ready")
    assert response.status_code == 503


async def test_cleanup_loop_runs_after_the_interval() -> None:
    pool = _Connection()
    sleeps = 0

    async def sleep(_seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _cleanup_loop(pool, 600, sleep=sleep)  # type: ignore[arg-type]
    assert sleeps == 2
    assert pool.queries == ["SELECT ingestion.delete_expired_idempotency_keys()"]


@asynccontextmanager
async def _reader(bootstrap: str) -> AsyncIterator[AIOKafkaConsumer]:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    partitions = [TopicPartition(TOPIC, index) for index in PARTITIONS]
    consumer.assign(partitions)
    await consumer.start()
    try:
        await consumer.seek_to_beginning(*partitions)
        yield consumer
    finally:
        await consumer.stop()


async def _read_for(
    consumer: AIOKafkaConsumer,
    event_id: str,
    wait_s: float,
) -> list[ConsumerRecord]:
    found: list[ConsumerRecord] = []
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        batches = await consumer.getmany(timeout_ms=200, max_records=100)
        for messages in batches.values():
            for message in messages:
                event = TransactionEvent.from_bytes(message.value)
                if str(event.event_id) == event_id:
                    found.append(message)
    return found


async def _wait_ready(client: AsyncClient) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        response = await client.get("/ready")
        if response.status_code == 200:
            return
        await asyncio.sleep(0.5)
    msg = "ingestion did not become ready"
    raise AssertionError(msg)


@pytest.mark.integration
async def test_post_publishes_within_one_second_and_retries_after_outage(
    kafka: KafkaService,
    postgres_dsn: str,
    tmp_path: Path,
) -> None:
    key_path = tmp_path / "hmac_key"
    key_path.write_text(bytes(range(32)).hex() + "\n", encoding="utf-8")
    app = create_app(
        Settings(
            kafka_bootstrap=kafka.bootstrap,
            database_url=postgres_dsn,
            hmac_key_path=str(key_path),
            cleanup_interval_s=3600,
            ack_timeout_s=3,
        )
    )
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/health")
            assert health.status_code == 200
            await _wait_ready(client)

            body = _body()
            async with _reader(kafka.bootstrap) as consumer:
                response = await client.post("/v1/events", json=body)
                assert response.status_code == 202
                assert response.json() == {"event_id": body["event_id"], "duplicate": False}
                records = await _read_for(consumer, str(body["event_id"]), 1)
                assert len(records) == 1
                assert VALID_PAN not in records[0].value.decode()
                assert records[0].key is not None

                duplicate = await client.post("/v1/events", json=body)
                assert duplicate.status_code == 202
                assert duplicate.json()["duplicate"] is True
                assert await _read_for(consumer, str(body["event_id"]), 1) == []

            outage = _body()
            await asyncio.to_thread(kafka.stop)
            try:
                failed = await client.post("/v1/events", json=outage)
                assert failed.status_code == 503
                assert failed.headers["retry-after"] == "1"
                assert str(outage["pan"]) not in failed.text
            finally:
                await asyncio.to_thread(kafka.start)
                await asyncio.to_thread(wait_for_kafka, kafka.bootstrap)
            await _wait_ready(client)
            retried = await client.post("/v1/events", json=outage)
            assert retried.status_code == 202
            assert retried.json()["duplicate"] is False
            async with _reader(kafka.bootstrap) as consumer:
                records = await _read_for(consumer, str(outage["event_id"]), 1)
            assert records
            assert VALID_PAN not in records[0].value.decode()
