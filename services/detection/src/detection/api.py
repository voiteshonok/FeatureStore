"""Alert API and the detection consumer.

One process serves the alert API and consumes ``features.updates``. Alert volume
is far below event volume, so the worker/API split in ADR-007 does not apply:
a separate consumer image would not isolate a hot path. The consumer task
starts in the application lifespan and commits offsets only after the alert
write (ADR-003).
"""

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, ConfigDict, Field

from detection.consumer import DetectionConsumer, configure_logging
from detection.metrics import METRICS, DetectionMetrics, start_metrics_server
from detection.rules import RuleCache
from detection.settings import Settings

_READY_TIMEOUT_S = 1.0
_DEFAULT_LIMIT = 100
_MAX_LIMIT = 1000
Status = Literal["open", "acknowledged", "resolved"]
_ALLOWED: frozenset[tuple[str, str]] = frozenset(
    {
        ("open", "acknowledged"),
        ("acknowledged", "resolved"),
        ("open", "resolved"),
    }
)

_LIST_OPEN = """
SELECT alert_id, rule_id, status, merchant_id, card_token, event_id,
       feature_snapshot, created_at, resolved_at, acknowledged_at
FROM detection.open_alerts_v
WHERE status = %s
ORDER BY created_at DESC
LIMIT %s
"""

_LIST_RESOLVED = """
SELECT alert_id, rule_id, status, merchant_id, card_token, event_id,
       feature_snapshot, created_at, resolved_at, acknowledged_at
FROM detection.alerts
WHERE status = 'resolved'
ORDER BY created_at DESC
LIMIT %s
"""

_LOCK = "SELECT status FROM detection.alerts WHERE alert_id = %s FOR UPDATE"

_RESOLVE = """
UPDATE detection.alerts
SET status = 'resolved',
    resolved_by = %s,
    resolved_at = now()
WHERE alert_id = %s
RETURNING alert_id, rule_id, status, merchant_id, card_token, event_id,
          feature_snapshot, created_at, resolved_at, acknowledged_at
"""

_ACKNOWLEDGE = """
UPDATE detection.alerts
SET status = 'acknowledged',
    acknowledged_by = %s,
    acknowledged_at = now()
WHERE alert_id = %s
RETURNING alert_id, rule_id, status, merchant_id, card_token, event_id,
          feature_snapshot, created_at, resolved_at, acknowledged_at
"""


class AlertUpdate(BaseModel):
    """Body of ``PATCH /v1/alerts/{alert_id}``."""

    model_config = ConfigDict(extra="forbid")

    status: Status
    operator_id: str = Field(min_length=1)


class AlertOut(BaseModel):
    """One alert row."""

    model_config = ConfigDict(extra="forbid")

    alert_id: UUID
    rule_id: UUID
    status: Status
    merchant_id: str | None
    card_token: str | None
    event_id: UUID
    feature_snapshot: dict[str, int | float]
    created_at: datetime
    resolved_at: datetime | None
    acknowledged_at: datetime | None


def transition_allowed(current: str, new: str) -> bool:
    """Return whether ``current`` may move to ``new``.

    Allowed paths are open to acknowledged, acknowledged to resolved, and
    open straight to resolved.
    """
    return (current, new) in _ALLOWED


def alert_from_row(row: Sequence[object]) -> AlertOut:
    """Build an alert from the shared select column order."""
    if len(row) != 10:
        msg = "alert row is incomplete"
        raise TypeError(msg)
    return AlertOut(
        alert_id=_uuid(row[0]),
        rule_id=_uuid(row[1]),
        status=_status(row[2]),
        merchant_id=_optional_text(row[3]),
        card_token=_optional_text(row[4]),
        event_id=_uuid(row[5]),
        feature_snapshot=_features(row[6]),
        created_at=_time(row[7]),
        resolved_at=_optional_time(row[8]),
        acknowledged_at=_optional_time(row[9]),
    )


async def _postgres_ready(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> bool:
    try:
        await asyncio.wait_for(_probe_postgres(pool), timeout=_READY_TIMEOUT_S)
    except Exception:
        return False
    return True


async def _probe_postgres(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


async def _list_alerts(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    status: Status,
    limit: int,
) -> list[AlertOut]:
    if status == "resolved":
        sql = _LIST_RESOLVED
        params: tuple[object, ...] = (limit,)
    else:
        sql = _LIST_OPEN
        params = (status, limit)
    async with pool.connection() as conn, conn.transaction():
        cursor = await conn.execute(sql, params)
        rows = await cursor.fetchall()
    return [alert_from_row(row) for row in rows]


async def _patch_alert(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    alert_id: UUID,
    body: AlertUpdate,
) -> AlertOut:
    async with pool.connection() as conn, conn.transaction():
        cursor = await conn.execute(_LOCK, (alert_id,))
        locked = await cursor.fetchone()
        if locked is None:
            raise HTTPException(status_code=404, detail="alert not found")
        current = locked[0]
        if not isinstance(current, str) or not transition_allowed(current, body.status):
            raise HTTPException(status_code=409, detail="invalid transition")
        if body.status == "resolved":
            sql = _RESOLVE
        elif body.status == "acknowledged":
            sql = _ACKNOWLEDGE
        else:
            raise HTTPException(status_code=409, detail="invalid transition")
        cursor = await conn.execute(sql, (body.operator_id, alert_id))
        updated = await cursor.fetchone()
        if updated is None:
            raise HTTPException(status_code=404, detail="alert not found")
        return alert_from_row(updated)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    start_metrics_server(settings.metrics_port, app.state.metrics.registry)
    configure_logging()
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]] = AsyncConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
    )
    cache = RuleCache()
    service: DetectionConsumer | None = None
    await pool.open(wait=True, timeout=30)
    try:
        service = await DetectionConsumer.connect(settings, pool, cache, metrics=app.state.metrics)
        await service.reload_with_retry()
        task = asyncio.create_task(service.run(), name="detection")
        app.state.service = service
        app.state.pool = pool
        app.state.consumer_task = task
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    finally:
        if service is not None:
            await service.aclose()
        await pool.close()


def create_app(
    settings: Settings | None = None,
    *,
    metrics: DetectionMetrics | None = None,
) -> FastAPI:
    """Build the detection API. Importing this module does not open connections."""
    app = FastAPI(title="detection", lifespan=_lifespan)
    app.state.settings = settings or Settings.from_env()
    app.state.metrics = METRICS if metrics is None else metrics

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(request: Request) -> JSONResponse:
        task = request.app.state.consumer_task
        service = request.app.state.service
        if task.done():
            kafka_ok = False
        else:
            kafka_ok = await service.ready()
        postgres_ok = await _postgres_ready(request.app.state.pool)
        body = {
            "status": "ready" if kafka_ok and postgres_ok else "not_ready",
            "kafka": "ok" if kafka_ok else "down",
            "postgres": "ok" if postgres_ok else "down",
        }
        code = 200 if kafka_ok and postgres_ok else 503
        return JSONResponse(status_code=code, content=body)

    @app.get("/v1/alerts", response_model=list[AlertOut])
    async def list_alerts(
        request: Request,
        status: Annotated[Status, Query()],
        limit: Annotated[int, Query(ge=1, le=_MAX_LIMIT)] = _DEFAULT_LIMIT,
    ) -> list[AlertOut]:
        return await _list_alerts(request.app.state.pool, status, limit)

    @app.patch(
        "/v1/alerts/{alert_id}",
        response_model=AlertOut,
        responses={
            404: {"description": "Unknown alert"},
            409: {"description": "Invalid transition"},
        },
    )
    async def patch_alert(alert_id: UUID, body: AlertUpdate, request: Request) -> AlertOut:
        return await _patch_alert(request.app.state.pool, alert_id, body)

    return app


def export_openapi(path: str) -> None:
    """Write the OpenAPI document. This does not connect to Kafka or Postgres."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(app.openapi(), indent=2) + "\n", encoding="utf-8")


def _uuid(value: object) -> UUID:
    if isinstance(value, UUID):
        return value
    msg = "expected uuid"
    raise TypeError(msg)


def _status(value: object) -> Status:
    if value == "open" or value == "acknowledged" or value == "resolved":
        return value
    msg = "alert status is invalid"
    raise TypeError(msg)


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    msg = "expected text"
    raise TypeError(msg)


def _features(value: object) -> dict[str, int | float]:
    if not isinstance(value, dict):
        msg = "feature_snapshot must be an object"
        raise TypeError(msg)
    parsed: dict[str, int | float] = {}
    for key, item in value.items():
        if not isinstance(key, str) or isinstance(item, bool) or not isinstance(item, int | float):
            msg = "feature_snapshot values must be numbers"
            raise TypeError(msg)
        parsed[key] = item
    return parsed


def _time(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    msg = "expected timestamp"
    raise TypeError(msg)


def _optional_time(value: object) -> datetime | None:
    if value is None:
        return None
    return _time(value)


app = create_app()
