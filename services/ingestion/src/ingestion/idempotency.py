"""Commit an event id only after Kafka has acknowledged the event."""

from collections.abc import Awaitable, Callable
from enum import Enum
from uuid import UUID

from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

from schemas import TransactionEvent

Publish = Callable[[TransactionEvent], Awaitable[None]]

_INSERT_KEY = """
INSERT INTO ingestion.idempotency_keys (event_id)
VALUES (%s)
ON CONFLICT DO NOTHING
RETURNING event_id
"""


class AcceptResult(Enum):
    """Outcome of :meth:`IdempotencyStore.accept`."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


class IdempotencyStore:
    """Insert ``event_id`` in the transaction that waits for the Kafka ack.

    The caller owns the pool (``open=False``, then ``await pool.open()``).
    A second call with the same ``event_id`` blocks inside the insert until
    the first transaction commits or rolls back.
    """

    def __init__(self, pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
        self._pool = pool

    async def accept(self, event: TransactionEvent, publish: Publish) -> AcceptResult:
        """Publish ``event`` once. Return ``DUPLICATE`` when ``event_id`` is already stored.

        ``publish`` runs before commit. Any exception rolls the key back and
        is re-raised, so a failed publish leaves no row.
        """
        async with self._pool.connection() as conn:
            async with conn.transaction():
                inserted = await _insert(conn, event.event_id)
                if inserted is None:
                    return AcceptResult.DUPLICATE
                await publish(event)
                return AcceptResult.ACCEPTED


async def _insert(conn: AsyncConnection[TupleRow], event_id: UUID) -> UUID | None:
    async with conn.cursor() as cur:
        await cur.execute(_INSERT_KEY, (event_id,))
        row = await cur.fetchone()
    if row is None:
        return None
    value = row[0]
    if not isinstance(value, UUID):
        msg = "idempotency insert did not return a UUID"
        raise TypeError(msg)
    return value
