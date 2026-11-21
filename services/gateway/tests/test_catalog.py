"""Registry SELECT and catalog failures."""

import httpx
from harness import mount, send


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]], *, fail: bool = False) -> None:
        self.rows = rows
        self.fail = fail
        self.sql = ""

    async def fetchall(self) -> list[tuple[object, ...]]:
        return list(self.rows)


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor = cursor

    async def execute(self, sql: str) -> _Cursor:
        self.cursor.sql = sql
        if self.cursor.fail:
            raise OSError("down")
        return self.cursor

    async def __aenter__(self) -> "_Connection":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor = cursor

    def connection(self) -> _Connection:
        return _Connection(self.cursor)


def _handler(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"ok": True})


_ROW = ("card_cnt_1m", "card", "stream", "count", 0, 1, 60_000, 5_000)
_ON_DEMAND = ("geo_velocity_kmh", "card", "on_demand", "geo_velocity", None, 1, None, None)


async def test_catalog_returns_the_registry_rows() -> None:
    cursor = _Cursor([_ROW, _ON_DEMAND])
    gateway = await mount(_handler, pool=_Pool(cursor))
    try:
        response = await send(gateway.app, "GET", "/v1/catalog")
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "card_cnt_1m",
            "entity": "card",
            "kind": "stream",
            "agg": "count",
            "default_value": 0.0,
            "version": 1,
            "window_ms": 60_000,
            "stale_after_ms": 5_000,
        },
        {
            "name": "geo_velocity_kmh",
            "entity": "card",
            "kind": "on_demand",
            "agg": "geo_velocity",
            "default_value": None,
            "version": 1,
            "window_ms": None,
            "stale_after_ms": None,
        },
    ]
    assert cursor.sql.count("SELECT") == 1
    assert "features.feature_definitions" in cursor.sql
    assert "features.feature_versions" in cursor.sql
    assert ";" not in cursor.sql


async def test_catalog_failure_is_503() -> None:
    gateway = await mount(_handler, pool=_Pool(_Cursor([], fail=True)))
    try:
        response = await send(gateway.app, "GET", "/v1/catalog")
    finally:
        await gateway.aclose()
    assert response.status_code == 503
    assert response.json()["type"] == "upstream_unavailable"
    assert response.json()["title"] == "catalog unavailable"
    assert response.json()["status"] == 503
    assert "request_id" in response.json()
