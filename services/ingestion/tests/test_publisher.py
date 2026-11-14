from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from aiokafka.errors import KafkaError

from ingestion import AcceptResult, IdempotencyStore, KafkaPublisher, PublishError
from schemas import TransactionEvent


def _event() -> TransactionEvent:
    now = datetime.now(UTC).replace(microsecond=0)
    return TransactionEvent.model_validate(
        {
            "event_id": uuid4(),
            "payment_id": "pay-1",
            "merchant_id": "m-1",
            "amount_minor": 100,
            "currency": "EUR",
            "status": "approved",
            "event_time": now,
            "card_token": "card-0",
            "ingest_time": now,
        }
    )


class _Transaction:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _Conn:
        return self._conn

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is None:
            self._conn.committed = True
        else:
            self._conn.rolled_back = True


class _Cursor:
    def __init__(self, row: tuple[UUID] | None) -> None:
        self._row = row
        self.query = ""
        self.params: tuple[object, ...] = ()

    async def __aenter__(self) -> _Cursor:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    async def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.query = query
        self.params = params

    async def fetchone(self) -> tuple[UUID] | None:
        return self._row


class _Conn:
    def __init__(self, row: tuple[UUID] | None) -> None:
        self.cursor_obj = _Cursor(row)
        self.committed = False
        self.rolled_back = False

    def transaction(self) -> _Transaction:
        return _Transaction(self)

    def cursor(self) -> _Cursor:
        return self.cursor_obj


class _Connection:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _Conn:
        return self._conn

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self._conn = conn

    def connection(self) -> _Connection:
        return _Connection(self._conn)


def _store(row: tuple[UUID] | None) -> tuple[IdempotencyStore, _Conn]:
    conn = _Conn(row)
    store = IdempotencyStore(_Pool(conn))  # type: ignore[arg-type]
    return store, conn


async def test_publish_sends_card_token_key_and_event_bytes() -> None:
    publisher = KafkaPublisher("127.0.0.1:9092")
    event = _event()
    recorded = _AsyncMock(return_value=object())
    publisher._producer.send_and_wait = recorded  # type: ignore[method-assign]
    try:
        await publisher.publish(event)
        producer = publisher._producer
        assert producer._sender._acks == -1
        assert producer._txn_manager is not None
        assert producer._message_accumulator._linger_time == 5 / 1000
        assert producer._request_timeout_ms == 10_000
    finally:
        await publisher.aclose()
    recorded.assert_awaited_once_with(
        "transactions.raw",
        value=event.to_bytes(),
        key=b"card-0",
    )


async def test_publish_timeout_raises_publish_error() -> None:
    publisher = KafkaPublisher("127.0.0.1:9092", ack_timeout_s=0.05)
    event = _event()

    async def hang(*args: object, **kwargs: object) -> None:
        await asyncio.sleep(30)

    publisher._producer.send_and_wait = hang  # type: ignore[method-assign]
    try:
        with pytest.raises(PublishError, match=str(event.event_id)) as exc_info:
            await publisher.publish(event)
    finally:
        await publisher.aclose()
    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_ready_follows_the_metadata_refresh() -> None:
    publisher = KafkaPublisher("127.0.0.1:9092")

    async def refresh(updated: bool) -> bool:
        return updated

    try:
        publisher._producer.client.force_metadata_update = lambda: refresh(True)  # type: ignore[method-assign]
        assert await publisher.ready() is True
        publisher._producer.client.force_metadata_update = lambda: refresh(False)  # type: ignore[method-assign]
        assert await publisher.ready() is False

        async def broken() -> bool:
            raise RuntimeError("down")

        publisher._producer.client.force_metadata_update = broken  # type: ignore[method-assign]
        assert await publisher.ready() is False
    finally:
        await publisher.aclose()


async def test_publish_broker_error_raises_publish_error() -> None:
    publisher = KafkaPublisher("127.0.0.1:9092", ack_timeout_s=1)
    event = _event()
    publisher._producer.send_and_wait = _AsyncMock(side_effect=KafkaError("down"))
    try:
        with pytest.raises(PublishError, match=str(event.event_id)) as exc_info:
            await publisher.publish(event)
    finally:
        await publisher.aclose()
    assert isinstance(exc_info.value.__cause__, KafkaError)


async def test_aclose_drops_the_sender_when_stop_times_out() -> None:
    publisher = KafkaPublisher("127.0.0.1:9092", ack_timeout_s=0.05)
    stop_calls = 0
    sender_closed = asyncio.Event()
    client_closed = asyncio.Event()
    real_sender_close = publisher._producer._sender.close
    real_client_close = publisher._producer.client.close

    async def stop_hangs() -> None:
        nonlocal stop_calls
        stop_calls += 1
        await asyncio.Event().wait()

    async def close_sender() -> None:
        sender_closed.set()
        await real_sender_close()

    async def close_client() -> None:
        client_closed.set()
        await real_client_close()

    publisher._producer.stop = stop_hangs  # type: ignore[method-assign]
    publisher._producer._sender.close = close_sender  # type: ignore[method-assign]
    publisher._producer.client.close = close_client  # type: ignore[method-assign]
    await publisher.aclose()
    await publisher.aclose()
    assert stop_calls == 1
    assert sender_closed.is_set()
    assert client_closed.is_set()


async def test_accept_publishes_a_new_event_and_commits() -> None:
    event = _event()
    store, conn = _store((event.event_id,))
    seen: list[TransactionEvent] = []

    async def publish(item: TransactionEvent) -> None:
        seen.append(item)

    assert await store.accept(event, publish) is AcceptResult.ACCEPTED
    assert seen == [event]
    assert conn.committed
    assert not conn.rolled_back
    assert "ON CONFLICT DO NOTHING" in conn.cursor_obj.query
    assert "RETURNING event_id" in conn.cursor_obj.query
    assert conn.cursor_obj.params == (event.event_id,)


async def test_accept_duplicate_does_not_publish() -> None:
    event = _event()
    store, conn = _store(None)

    async def publish(item: TransactionEvent) -> None:
        raise AssertionError(item.event_id)

    assert await store.accept(event, publish) is AcceptResult.DUPLICATE
    assert conn.committed
    assert not conn.rolled_back


async def test_accept_rolls_back_when_publish_fails() -> None:
    event = _event()
    store, conn = _store((event.event_id,))

    async def publish(item: TransactionEvent) -> None:
        raise PublishError(f"failed to publish event {item.event_id}")

    with pytest.raises(PublishError):
        await store.accept(event, publish)
    assert conn.rolled_back
    assert not conn.committed


class _AsyncMock:
    """Tiny awaitable spy so the unit tests do not depend on unittest.mock."""

    def __init__(
        self,
        *,
        return_value: object = None,
        side_effect: BaseException | None = None,
    ) -> None:
        self._return_value = return_value
        self._side_effect = side_effect
        self.await_args: tuple[tuple[object, ...], dict[str, object]] | None = None

    def assert_awaited_once_with(self, *args: object, **kwargs: object) -> None:
        assert self.await_args == (args, kwargs)

    async def __call__(self, *args: object, **kwargs: object) -> object:
        self.await_args = (args, kwargs)
        if self._side_effect is not None:
            raise self._side_effect
        return self._return_value
