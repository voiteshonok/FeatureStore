"""Read-only feature registry query (ADR-008).

One SELECT. ``svc_gateway`` has ``SELECT`` on these two tables and nothing else.
"""

import logging
from collections.abc import Sequence

from psycopg import AsyncConnection, Error
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool

_CATALOG_SQL = """
SELECT d.name, d.entity, d.kind, d.agg, d.default_value,
       v.version, v.window_ms, v.stale_after_ms
FROM features.feature_definitions AS d
JOIN features.feature_versions AS v USING (name)
ORDER BY d.name, v.version
"""


class CatalogUnavailable(Exception):
    """The registry SELECT failed."""


async def read_catalog(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
) -> list[dict[str, object]]:
    """Return every registry version row. Raises ``CatalogUnavailable`` on failure."""
    try:
        async with pool.connection() as conn:
            cursor = await conn.execute(_CATALOG_SQL)
            rows = await cursor.fetchall()
    except (Error, OSError):
        logging.getLogger("gateway").warning("catalog read failed", exc_info=True)
        raise CatalogUnavailable from None
    return [_entry(row) for row in rows]


def _entry(row: Sequence[object]) -> dict[str, object]:
    if len(row) != 8:
        msg = "catalog row is incomplete"
        raise TypeError(msg)
    return {
        "name": _text(row[0]),
        "entity": _text(row[1]),
        "kind": _text(row[2]),
        "agg": _text(row[3]),
        "default_value": _optional_float(row[4]),
        "version": _int(row[5]),
        "window_ms": _optional_int(row[6]),
        "stale_after_ms": _optional_int(row[7]),
    }


def _text(value: object) -> str:
    if isinstance(value, str):
        return value
    msg = "catalog text column is invalid"
    raise TypeError(msg)


def _int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "catalog integer column is invalid"
        raise TypeError(msg)
    return value


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return _int(value)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        msg = "catalog default_value is invalid"
        raise TypeError(msg)
    return float(value)
