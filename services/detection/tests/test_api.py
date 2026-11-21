"""Health endpoints, transitions, roles, and keyset cursors."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from detection.api import AlertUpdate, create_app, transition_allowed
from detection.services.cursor import CursorError, decode_cursor, encode_cursor
from detection.services.transitions import can_mutate
from detection.settings import Settings

_NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
_ALERT = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


class _Engine:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    def connect(self) -> _Engine:
        return self

    async def execute(self, _stmt: object) -> None:
        if self.fail:
            raise OSError("down")

    async def __aenter__(self) -> _Engine:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


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
    app.state.engine = _Engine(fail=not postgres)
    app.state.consumer_task = _Task(task_done)
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
    ("role", "allowed"),
    [
        ("viewer", False),
        ("analyst", True),
        ("admin", True),
        ("unknown", False),
    ],
)
def test_role_matrix(role: str, allowed: bool) -> None:
    assert can_mutate(role) is allowed


def test_cursor_roundtrip() -> None:
    encoded = encode_cursor(_NOW, _ALERT)
    decoded = decode_cursor(encoded)
    assert decoded.created_at == _NOW
    assert decoded.alert_id == _ALERT


@pytest.mark.parametrize("value", ["", "%%%", "not-base64", "e30="])
def test_cursor_rejects_garbage(value: str) -> None:
    with pytest.raises(CursorError):
        decode_cursor(value)


def test_update_model_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError):
        AlertUpdate.model_validate({"status": "resolved", "note": "done ok", "extra": 1})


def test_update_model_requires_note_length_when_set() -> None:
    with pytest.raises(ValueError):
        AlertUpdate.model_validate({"status": "resolved", "note": "ab"})
