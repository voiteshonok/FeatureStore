"""Postgres integration for the layered alerts API (T7.1)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient

from detection.api import create_app
from detection.db import make_engine, make_session_factory
from detection.settings import Settings

pytestmark = pytest.mark.integration

_OPERATOR = {"X-Operator-Id": "op-1", "X-Operator-Role": "analyst"}
_VIEWER = {"X-Operator-Id": "viewer-1", "X-Operator-Role": "viewer"}


async def _insert_alert(
    dsn: str,
    *,
    rule_id: UUID,
    subject: str,
    status: str = "open",
    created_at: datetime,
    event_id: UUID | None = None,
) -> UUID:
    alert_id = uuid4()
    event = event_id or uuid4()
    resolved_at = created_at if status == "resolved" else None
    resolved_by = "seed" if status == "resolved" else None
    note = "seed note" if status == "resolved" else None
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            """
            INSERT INTO detection.alerts (
                alert_id, rule_id, card_token, event_id, feature_snapshot,
                status, created_at, resolved_at, resolved_by, resolution_note
            ) VALUES (
                %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s
            )
            """,
            (
                alert_id,
                rule_id,
                subject,
                event,
                '{"card_cnt_1m": 6}',
                status,
                created_at,
                resolved_at,
                resolved_by,
                note,
            ),
        )
    return alert_id


async def _seed_rule(dsn: str) -> UUID:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        row = await conn.execute("SELECT rule_id FROM detection.alert_rules ORDER BY name LIMIT 1")
        found = await row.fetchone()
    assert found is not None
    assert isinstance(found[0], UUID)
    return found[0]


def _api_app(database_url: str) -> object:
    """App with ORM session factory but no Kafka consumer."""
    settings = Settings(database_url=database_url, metrics_port=0)
    app = create_app(settings)
    engine = make_engine(database_url)
    app.state.engine = engine
    app.state.session_factory = make_session_factory(engine)
    app.state.service = _AlwaysReady()
    app.state.consumer_task = _RunningTask()
    return app


class _AlwaysReady:
    async def ready(self) -> bool:
        return True


class _RunningTask:
    def done(self) -> bool:
        return False


@pytest.fixture
async def api_client(database: object) -> AsyncClient:
    app = _api_app(database.service_dsn)  # type: ignore[attr-defined]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await app.state.engine.dispose()  # type: ignore[attr-defined]


async def test_list_filters_sort_and_keyset(api_client: AsyncClient, database: object) -> None:
    dsn = database.superuser_dsn  # type: ignore[attr-defined]
    rule_id = await _seed_rule(dsn)
    base = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    for index in range(5):
        await _insert_alert(
            dsn,
            rule_id=rule_id,
            subject=f"card-{index}",
            created_at=base + timedelta(seconds=index),
        )
    # Same created_at, different alert_id — keyset must not skip/duplicate.
    twin_time = base + timedelta(seconds=10)
    twin_a = await _insert_alert(dsn, rule_id=rule_id, subject="twin-a", created_at=twin_time)
    twin_b = await _insert_alert(dsn, rule_id=rule_id, subject="twin-b", created_at=twin_time)

    first = await api_client.get(
        "/v1/alerts",
        params=[("status", "open"), ("limit", "3"), ("sort", "-created_at")],
    )
    assert first.status_code == 200, first.text
    body = first.json()
    assert len(body["items"]) == 3
    assert body["next_cursor"] is not None

    second = await api_client.get(
        "/v1/alerts",
        params={
            "status": "open",
            "limit": 10,
            "sort": "-created_at",
            "cursor": body["next_cursor"],
        },
    )
    assert second.status_code == 200
    page_ids = [item["alert_id"] for item in body["items"]] + [
        item["alert_id"] for item in second.json()["items"]
    ]
    assert len(page_ids) == len(set(page_ids))
    assert str(twin_a) in page_ids and str(twin_b) in page_ids

    filtered = await api_client.get(
        "/v1/alerts",
        params={"entity": "card-1", "rule_id": str(rule_id)},
    )
    assert filtered.status_code == 200
    assert len(filtered.json()["items"]) == 1
    assert filtered.json()["items"][0]["card_token"] == f"card-{1}"

    bad = await api_client.get("/v1/alerts", params={"cursor": "not-a-cursor"})
    assert bad.status_code == 422


async def test_get_detail_and_rules(api_client: AsyncClient, database: object) -> None:
    dsn = database.superuser_dsn  # type: ignore[attr-defined]
    rule_id = await _seed_rule(dsn)
    alert_id = await _insert_alert(
        dsn,
        rule_id=rule_id,
        subject="detail-card",
        created_at=datetime.now(UTC),
    )
    detail = await api_client.get(f"/v1/alerts/{alert_id}")
    assert detail.status_code == 200
    payload = detail.json()
    assert payload["alert"]["alert_id"] == str(alert_id)
    assert payload["rule"]["rule_id"] == str(rule_id)
    assert payload["rule"]["conditions"]
    assert payload["history"] == []

    missing = await api_client.get(f"/v1/alerts/{uuid4()}")
    assert missing.status_code == 404

    rules = await api_client.get("/v1/rules")
    assert rules.status_code == 200
    assert any(row["rule_id"] == str(rule_id) for row in rules.json())


async def test_patch_writes_note_to_audit_and_viewer_forbidden(
    api_client: AsyncClient, database: object
) -> None:
    dsn = database.superuser_dsn  # type: ignore[attr-defined]
    rule_id = await _seed_rule(dsn)
    alert_id = await _insert_alert(
        dsn,
        rule_id=rule_id,
        subject="patch-card",
        created_at=datetime.now(UTC),
    )
    denied = await api_client.patch(
        f"/v1/alerts/{alert_id}",
        headers=_VIEWER,
        json={"status": "acknowledged"},
    )
    assert denied.status_code == 403

    ack = await api_client.patch(
        f"/v1/alerts/{alert_id}",
        headers=_OPERATOR,
        json={"status": "acknowledged"},
    )
    assert ack.status_code == 200
    assert ack.json()["status"] == "acknowledged"

    missing_note = await api_client.patch(
        f"/v1/alerts/{alert_id}",
        headers=_OPERATOR,
        json={"status": "resolved"},
    )
    assert missing_note.status_code == 422

    resolved = await api_client.patch(
        f"/v1/alerts/{alert_id}",
        headers=_OPERATOR,
        json={"status": "resolved", "note": "false positive"},
    )
    assert resolved.status_code == 200
    assert resolved.json()["status"] == "resolved"
    assert resolved.json()["resolution_note"] == "false positive"

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        cursor = await conn.execute(
            """
            SELECT actor, action, detail->>'resolution_note'
            FROM ops.audit_log
            WHERE alert_id = %s
            ORDER BY audit_id
            """,
            (alert_id,),
        )
        rows = await cursor.fetchall()
    assert rows[-1] == ("op-1", "resolved", "false positive")

    detail = await api_client.get(f"/v1/alerts/{alert_id}")
    assert len(detail.json()["history"]) == 2


async def test_concurrent_patch_one_wins(api_client: AsyncClient, database: object) -> None:
    dsn = database.superuser_dsn  # type: ignore[attr-defined]
    rule_id = await _seed_rule(dsn)
    alert_id = await _insert_alert(
        dsn,
        rule_id=rule_id,
        subject="race-card",
        created_at=datetime.now(UTC),
    )

    async def _ack() -> int:
        response = await api_client.patch(
            f"/v1/alerts/{alert_id}",
            headers=_OPERATOR,
            json={"status": "acknowledged"},
        )
        return response.status_code

    codes = await asyncio.gather(_ack(), _ack())
    assert sorted(codes) == [200, 409]
