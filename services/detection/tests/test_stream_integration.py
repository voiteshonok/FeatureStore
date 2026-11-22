"""PostgreSQL NOTIFY → SSE integration (ADR-021)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from detection.db import make_engine, make_session_factory
from detection.live.broadcast import BroadcastHub
from detection.live.listener import AlertNotifyListener
from detection.live.sse import sse_frames

pytestmark = pytest.mark.integration

_POSTGRES = Path(__file__).resolve().parents[3] / "infra" / "postgres"
_POSTGRES_IMAGE = "postgres:17"
_RULE = UUID("c0ffee00-0000-4000-8000-000000000001")

_ROLES_SQL = """
CREATE ROLE svc_ingestion LOGIN PASSWORD 'svc_ingestion_dev';
CREATE ROLE svc_features LOGIN PASSWORD 'svc_features_dev';
CREATE ROLE svc_detection LOGIN PASSWORD 'svc_detection_dev';
CREATE ROLE svc_gateway LOGIN PASSWORD 'svc_gateway_dev';
"""


def _dsn(postgres: PostgresContainer, *, user: str, password: str) -> str:
    return psycopg.conninfo.make_conninfo(
        host=postgres.get_container_host_ip(),
        port=postgres.get_exposed_port(postgres.port),
        user=user,
        password=password,
        dbname=postgres.dbname,
    )


def _apply(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(_ROLES_SQL)
            cur.execute((_POSTGRES / "10-schema.sql").read_text())
            cur.execute((_POSTGRES / "20-seed.sql").read_text())
            cur.execute((_POSTGRES / "40-alerts-ui.sql").read_text())


@dataclass(frozen=True)
class Database:
    superuser_dsn: str
    service_dsn: str


@pytest.fixture(scope="module")
def database() -> Iterator[Database]:
    with PostgresContainer(_POSTGRES_IMAGE, driver=None) as postgres:
        superuser = _dsn(postgres, user=postgres.username, password=postgres.password)
        _apply(superuser)
        service = _dsn(
            postgres,
            user="svc_detection",
            password="svc_detection_dev",  # noqa: S106
        )
        yield Database(superuser, service)


async def _collect_alert_event(hub: BroadcastHub, timeout_s: float = 2.0) -> dict[str, object]:
    async def read() -> dict[str, object]:
        async for chunk in sse_frames(hub, last_event_id=None):
            text = chunk.decode()
            if "event: alert" in text:
                for line in text.splitlines():
                    if line.startswith("data: "):
                        return json.loads(line[6:])
        msg = "stream ended without alert"
        raise AssertionError(msg)

    return await asyncio.wait_for(read(), timeout=timeout_s)


async def _run_listener(
    database: Database,
) -> tuple[BroadcastHub, AlertNotifyListener, asyncio.Task[None], object]:
    engine = make_engine(database.service_dsn)
    session_factory = make_session_factory(engine)
    hub = BroadcastHub()
    listener = AlertNotifyListener(database.service_dsn, session_factory, hub)
    task = asyncio.create_task(listener.run())
    await asyncio.sleep(0.4)
    return hub, listener, task, engine


async def _stop(
    listener: AlertNotifyListener,
    task: asyncio.Task[None],
    engine: object,
) -> None:
    listener.stop()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    await engine.dispose()  # type: ignore[attr-defined]


def _insert_alert(
    dsn: str,
    alert_id: UUID,
    *,
    card_token: str | None = None,
    commit: bool = True,
) -> None:
    token = card_token if card_token is not None else f"card{uuid4().hex}"
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO detection.alerts
                    (alert_id, rule_id, card_token, event_id, feature_snapshot, status)
                VALUES
                    (%s, %s, %s, %s, %s::jsonb, 'open')
                """,
                (
                    str(alert_id),
                    str(_RULE),
                    token,
                    str(uuid4()),
                    json.dumps({"card_cnt_1m": 6}),
                ),
            )
        if commit:
            conn.commit()
        else:
            conn.rollback()


@pytest.mark.asyncio
async def test_insert_reaches_sse_within_one_second(database: Database) -> None:
    hub, listener, task, engine = await _run_listener(database)
    alert_id = uuid4()
    try:
        collector = asyncio.create_task(_collect_alert_event(hub))
        await asyncio.sleep(0.05)
        card = "abcdefghijklmnop"
        _insert_alert(database.service_dsn, alert_id, card_token=card, commit=True)
        payload = await asyncio.wait_for(collector, timeout=1.0)
        assert payload["op"] == "created"
        assert payload["alert_id"] == str(alert_id)
        assert payload["entity_id"] == "abcd…op"
        assert payload["rule_name"] == "card_velocity_burst"
    finally:
        await _stop(listener, task, engine)


@pytest.mark.asyncio
async def test_rollback_does_not_notify(database: Database) -> None:
    hub, listener, task, engine = await _run_listener(database)
    try:
        collector = asyncio.create_task(_collect_alert_event(hub, timeout_s=0.8))
        await asyncio.sleep(0.05)
        _insert_alert(database.service_dsn, uuid4(), commit=False)
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            await collector
    finally:
        await _stop(listener, task, engine)


@pytest.mark.asyncio
async def test_status_update_emits_updated(database: Database) -> None:
    hub, listener, task, engine = await _run_listener(database)
    alert_id = uuid4()
    try:
        _insert_alert(database.service_dsn, alert_id, commit=True)
        await asyncio.sleep(0.3)
        collector = asyncio.create_task(_collect_alert_event(hub))
        await asyncio.sleep(0.05)
        with psycopg.connect(database.service_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE detection.alerts
                    SET status = 'acknowledged',
                        acknowledged_by = 'op-1',
                        acknowledged_at = now()
                    WHERE alert_id = %s
                    """,
                    (str(alert_id),),
                )
            conn.commit()
        payload = await asyncio.wait_for(collector, timeout=1.0)
        assert payload["op"] == "updated"
        assert payload["status"] == "acknowledged"
        assert payload["alert_id"] == str(alert_id)
    finally:
        await _stop(listener, task, engine)
