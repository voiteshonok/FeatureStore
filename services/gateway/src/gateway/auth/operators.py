"""Read operators for login (column grant only; ADR-020)."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from psycopg import AsyncConnection, Error
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

_BY_EMAIL = """
SELECT operator_id, email, password_hash, role
FROM ops.operators
WHERE email = %s
"""

_BY_ID = """
SELECT operator_id, email, role
FROM ops.operators
WHERE operator_id = %s
"""


@dataclass(frozen=True)
class OperatorRow:
    """Columns the gateway is allowed to see."""

    operator_id: int
    email: str
    role: str
    password_hash: str | None = None


class OperatorsUnavailable(Exception):
    """The operators SELECT failed."""


async def fetch_operator_by_email(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    email: str,
) -> OperatorRow | None:
    """Return the operator for ``email``, or ``None`` when missing."""
    try:
        async with pool.connection() as conn:
            cursor = await conn.execute(_BY_EMAIL, (email.lower(),))
            row = await cursor.fetchone()
    except (Error, OSError):
        logging.getLogger("gateway").warning("operator lookup failed", exc_info=True)
        raise OperatorsUnavailable from None
    if row is None:
        return None
    return OperatorRow(
        operator_id=_int(row[0]),
        email=_text(row[1]),
        password_hash=_text(row[2]),
        role=_text(row[3]),
    )


async def fetch_operator_by_id(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    operator_id: str,
) -> OperatorRow | None:
    """Return email and role for ``operator_id`` (for ``/v1/auth/me``)."""
    try:
        oid = int(operator_id)
    except ValueError:
        return None
    try:
        async with pool.connection() as conn:
            cursor = await conn.execute(_BY_ID, (oid,))
            row = await cursor.fetchone()
    except (Error, OSError):
        logging.getLogger("gateway").warning("operator lookup failed", exc_info=True)
        raise OperatorsUnavailable from None
    if row is None:
        return None
    return OperatorRow(
        operator_id=_int(row[0]),
        email=_text(row[1]),
        role=_text(row[2]),
    )


def _text(value: object) -> str:
    if isinstance(value, str):
        return value
    msg = "operator text column is invalid"
    raise TypeError(msg)


def _int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "operator id is invalid"
        raise TypeError(msg)
    return value
