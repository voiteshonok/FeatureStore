"""Kafka and Postgres coverage for one open alert per rule and entity."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from uuid import UUID

import psycopg
import pytest
from aiokafka import AIOKafkaProducer
from detection_stack import GROUP, TOPIC, group_offsets
from httpx import ASGITransport, AsyncClient

from detection.api import create_app
from detection.settings import Settings
from schemas import FeatureSnapshot

pytestmark = pytest.mark.integration

_EVENT = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
_ALERTS = """
SELECT alert_id::text, card_token, feature_snapshot, status
FROM detection.alerts
ORDER BY created_at
"""
_RESOLVED = """
SELECT resolved_at IS NOT NULL, resolved_by, status
FROM detection.alerts
WHERE alert_id = %s
"""
_AUDIT = "SELECT count(*) FROM ops.audit_log"


def _payload() -> bytes:
    now = datetime.now(UTC).replace(microsecond=0)
    snapshot = FeatureSnapshot(
        entity_type="card",
        entity_id="card-1",
        event_id=_EVENT,
        event_time=now,
        ingest_time=now,
        features={"card_cnt_1m": 6, "card_cnt_1h": 3},
        source_partition=0,
        source_offset=0,
    )
    return snapshot.to_bytes()


async def _publish(bootstrap: str, payload: bytes) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap, client_id="detection-tests")
    await producer.start()
    try:
        await producer.send_and_wait(TOPIC, value=payload, key=b"card-1")
    finally:
        await producer.stop()


async def _rows(dsn: str, sql: str, params: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())


async def _wait_alerts(app: object, dsn: str, expected: int) -> list[tuple[object, ...]]:
    task = app.state.consumer_task  # type: ignore[attr-defined]
    deadline = time.monotonic() + 30
    last: list[tuple[object, ...]] = []
    while time.monotonic() < deadline:
        if task.done():
            msg = f"consumer stopped: {task.exception()}"
            raise AssertionError(msg)
        last = await _rows(dsn, _ALERTS)
        if len(last) >= expected:
            return last
        await asyncio.sleep(0.2)
    msg = f"expected {expected} alerts, saw {last}"
    raise AssertionError(msg)


async def _wait_offset(app: object, bootstrap: str, expected: int) -> None:
    task = app.state.consumer_task  # type: ignore[attr-defined]
    deadline = time.monotonic() + 30
    last: dict[int, int] | None = None
    while time.monotonic() < deadline:
        if task.done():
            msg = f"consumer stopped: {task.exception()}"
            raise AssertionError(msg)
        last = await asyncio.to_thread(group_offsets, bootstrap)
        if last is not None and sum(last.values()) >= expected:
            return
        await asyncio.sleep(0.2)
    msg = f"group {GROUP} offsets did not reach {expected}: {last}"
    raise AssertionError(msg)


async def test_redelivery_collapses_and_resolve_writes_one_audit_row(
    kafka: object,
    database: object,
) -> None:
    bootstrap = kafka.bootstrap  # type: ignore[attr-defined]
    settings = Settings(
        kafka_bootstrap=bootstrap,
        database_url=database.service_dsn,  # type: ignore[attr-defined]
        rule_reload_s=3600,
    )
    app = create_app(settings)
    payload = _payload()
    async with app.router.lifespan_context(app):
        await _publish(bootstrap, payload)
        rows = await _wait_alerts(app, database.superuser_dsn, 1)  # type: ignore[attr-defined]
        assert len(rows) == 1
        alert_id, stored_card, features, status = rows[0]
        assert stored_card == "card-1"
        assert status == "open"
        assert set(features) == {"card_cnt_1m"}
        assert float(features["card_cnt_1m"]) == 6

        await _publish(bootstrap, payload)
        await _wait_offset(app, bootstrap, 2)
        again = await _rows(database.superuser_dsn, _ALERTS)  # type: ignore[attr-defined]
        assert len(again) == 1

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            ready = await client.get("/ready")
            opened = await client.get("/v1/alerts", params={"status": "open"})
            response = await client.patch(
                f"/v1/alerts/{alert_id}",
                json={"status": "resolved", "operator_id": "op-1"},
            )
            resolved = await client.get("/v1/alerts", params={"status": "resolved"})
        assert ready.status_code == 200, ready.text
        assert ready.json() == {"status": "ready", "kafka": "ok", "postgres": "ok"}
        assert opened.status_code == 200
        assert len(opened.json()) == 1
        assert response.status_code == 200
        assert response.json()["status"] == "resolved"
        assert resolved.status_code == 200
        assert len(resolved.json()) == 1

        stored = await _rows(
            database.superuser_dsn,  # type: ignore[attr-defined]
            _RESOLVED,
            (alert_id,),
        )
        assert stored == [(True, "op-1", "resolved")]
        audit = await _rows(database.superuser_dsn, _AUDIT)  # type: ignore[attr-defined]
        assert audit == [(1,)]
