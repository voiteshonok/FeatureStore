"""Alert transitions and the health endpoints."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from detection.api import AlertUpdate, create_app, transition_allowed
from detection.settings import Settings

_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
_ALERT = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_RULE = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
_EVENT = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")


def _alert_row(status: str) -> tuple[object, ...]:
    resolved = _NOW if status == "resolved" else None
    acknowledged = _NOW if status == "acknowledged" else None
    return (
        _ALERT,
        _RULE,
        status,
        None,
        "card-1",
        _EVENT,
        {"card_cnt_1m": 6},
        _NOW,
        resolved,
        acknowledged,
    )


class _Cursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    async def fetchone(self) -> tuple[object, ...] | None:
        if not self._rows:
            return None
        return self._rows[0]

    async def fetchall(self) -> list[tuple[object, ...]]:
        return list(self._rows)


class _Conn:
    def __init__(
        self,
        *,
        status: str | None = "open",
        rows: list[tuple[object, ...]] | None = None,
        fail: bool = False,
    ) -> None:
        self.status = status
        self.rows = rows if rows is not None else [_alert_row("open")]
        self.fail = fail
        self.sql: list[str] = []

    async def execute(self, sql: str, _params: object = None) -> _Cursor:
        if self.fail:
            raise OSError("down")
        self.sql.append(sql)
        if "FOR UPDATE" in sql:
            if self.status is None:
                return _Cursor([])
            return _Cursor([(self.status,)])
        if "UPDATE detection.alerts" in sql:
            new_status = "resolved" if "status = 'resolved'" in sql else "acknowledged"
            return _Cursor([_alert_row(new_status)])
        return _Cursor(self.rows)

    def transaction(self) -> _Conn:
        return self

    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _Pool:
    def __init__(self, conn: _Conn) -> None:
        self.conn = conn

    def connection(self) -> _Conn:
        return self.conn


class _Service:
    def __init__(self, ok: bool) -> None:
        self.ok = ok

    async def ready(self) -> bool:
        return self.ok


class _Task:
    def __init__(self, done: bool) -> None:
        self._done = done

    def done(self) -> bool:
        return self._done


def _client(app: FastAPI) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _ready_app(*, kafka: bool, postgres: bool, task_done: bool = False) -> FastAPI:
    app = create_app(Settings())
    app.state.service = _Service(kafka)
    app.state.pool = _Pool(_Conn(fail=not postgres))
    app.state.consumer_task = _Task(task_done)
    return app


def _patch_app(conn: _Conn) -> FastAPI:
    app = create_app(Settings())
    app.state.pool = _Pool(conn)
    return app


async def test_health_does_not_check_dependencies() -> None:
    async with _client(create_app(Settings())) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_ready_reports_each_probe() -> None:
    async with _client(_ready_app(kafka=True, postgres=True)) as client:
        ready = await client.get("/ready")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready", "kafka": "ok", "postgres": "ok"}

    async with _client(_ready_app(kafka=False, postgres=True)) as client:
        kafka_down = await client.get("/ready")
    assert kafka_down.status_code == 503
    assert kafka_down.json()["kafka"] == "down"
    assert kafka_down.json()["postgres"] == "ok"

    async with _client(_ready_app(kafka=True, postgres=False)) as client:
        postgres_down = await client.get("/ready")
    assert postgres_down.status_code == 503
    assert postgres_down.json()["postgres"] == "down"

    async with _client(_ready_app(kafka=True, postgres=True, task_done=True)) as client:
        stopped = await client.get("/ready")
    assert stopped.status_code == 503
    assert stopped.json()["kafka"] == "down"


@pytest.mark.parametrize(
    ("current", "new"),
    [
        ("acknowledged", "open"),
        ("resolved", "acknowledged"),
        ("resolved", "open"),
        ("open", "open"),
        ("acknowledged", "acknowledged"),
        ("resolved", "resolved"),
    ],
)
def test_rejected_transitions(current: str, new: str) -> None:
    assert transition_allowed(current, new) is False


@pytest.mark.parametrize(
    ("current", "new"),
    [
        ("open", "acknowledged"),
        ("acknowledged", "resolved"),
        ("open", "resolved"),
    ],
)
def test_allowed_transitions(current: str, new: str) -> None:
    assert transition_allowed(current, new) is True


@pytest.mark.parametrize(
    ("current", "new"),
    [
        ("acknowledged", "open"),
        ("resolved", "acknowledged"),
        ("open", "open"),
    ],
)
async def test_invalid_transition_is_409(current: str, new: str) -> None:
    conn = _Conn(status=current)
    async with _client(_patch_app(conn)) as client:
        response = await client.patch(
            f"/v1/alerts/{_ALERT}",
            json={"status": new, "operator_id": "op-1"},
        )
    assert response.status_code == 409
    assert response.json()["detail"] == "invalid transition"
    assert not any("UPDATE detection.alerts" in sql for sql in conn.sql)


@pytest.mark.parametrize("new", ["acknowledged", "resolved"])
async def test_allowed_patch_updates_the_row(new: str) -> None:
    current = "open" if new == "acknowledged" else "acknowledged"
    conn = _Conn(status=current)
    async with _client(_patch_app(conn)) as client:
        response = await client.patch(
            f"/v1/alerts/{_ALERT}",
            json={"status": new, "operator_id": "op-1"},
        )
    assert response.status_code == 200
    assert response.json()["status"] == new
    assert any("UPDATE detection.alerts" in sql for sql in conn.sql)


async def test_unknown_alert_is_404() -> None:
    conn = _Conn(status=None)
    async with _client(_patch_app(conn)) as client:
        response = await client.patch(
            f"/v1/alerts/{_ALERT}",
            json={"status": "resolved", "operator_id": "op-1"},
        )
    assert response.status_code == 404


async def test_open_alerts_come_from_the_view_and_resolved_from_the_table() -> None:
    open_conn = _Conn(rows=[_alert_row("open")])
    async with _client(_patch_app(open_conn)) as client:
        opened = await client.get("/v1/alerts", params={"status": "open", "limit": 10})
    assert opened.status_code == 200
    assert opened.json()[0]["alert_id"] == str(_ALERT)
    assert "open_alerts_v" in open_conn.sql[0]

    resolved_conn = _Conn(rows=[_alert_row("resolved")])
    async with _client(_patch_app(resolved_conn)) as client:
        resolved = await client.get("/v1/alerts", params={"status": "resolved"})
    assert resolved.status_code == 200
    assert resolved.json()[0]["status"] == "resolved"
    assert "open_alerts_v" not in resolved_conn.sql[0]
    assert "status = 'resolved'" in resolved_conn.sql[0]


async def test_limit_and_operator_are_validated() -> None:
    async with _client(create_app(Settings())) as client:
        limit = await client.get("/v1/alerts", params={"status": "open", "limit": 0})
        body = await client.patch(
            f"/v1/alerts/{_ALERT}",
            json={"status": "resolved", "operator_id": ""},
        )
    assert limit.status_code == 422
    assert body.status_code == 422


def test_update_model_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError):
        AlertUpdate.model_validate({"status": "open", "operator_id": "op-1", "extra": 1})
