"""Detection application assembly (ADR-019 / ADR-021).

One process serves the alert API, consumes ``features.updates``, and fans out
the live SSE stream. Alert volume is far below event volume, so the worker/API
split in ADR-007 does not apply. The consumer task starts in the application
lifespan and commits offsets only after the alert write (ADR-003).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from detection.api.alerts import router as alerts_router
from detection.api.rules import router as rules_router
from detection.api.stream import router as stream_router
from detection.consumer import DetectionConsumer, configure_logging
from detection.db import make_engine, make_session_factory
from detection.errors import install_error_handlers, install_request_id_middleware
from detection.live.broadcast import BroadcastHub
from detection.live.listener import AlertNotifyListener, run_stats_ticker
from detection.live.stats import LiveStats
from detection.metrics import METRICS, DetectionMetrics, start_metrics_server
from detection.rules import RuleCache
from detection.settings import Settings

_READY_TIMEOUT_S = 1.0


async def _postgres_ready(engine: AsyncEngine) -> bool:
    try:
        await asyncio.wait_for(_probe_postgres(engine), timeout=_READY_TIMEOUT_S)
    except Exception:
        return False
    return True


async def _probe_postgres(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    metrics: DetectionMetrics = app.state.metrics
    start_metrics_server(settings.metrics_port, metrics.registry)
    configure_logging()
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]] = AsyncConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
    )
    engine = make_engine(settings.database_url)
    session_factory: async_sessionmaker[AsyncSession] = make_session_factory(engine)
    cache = RuleCache()
    stats = LiveStats()
    hub = BroadcastHub(
        on_subscribers_changed=metrics.stream_subscribers.set,
        on_dropped=metrics.stream_dropped.inc,
    )
    app.state.stats = stats
    app.state.broadcast = hub
    service: DetectionConsumer | None = None
    await pool.open(wait=True, timeout=30)
    app.state.pool = pool
    app.state.engine = engine
    app.state.session_factory = session_factory
    listener = AlertNotifyListener(
        settings.database_url,
        session_factory,
        hub,
        on_delivered=metrics.stream_delivery.observe,
    )
    try:
        service = await DetectionConsumer.connect(
            settings,
            pool,
            cache,
            metrics=metrics,
            stats=stats,
        )
        await service.reload_with_retry()
        consumer_task = asyncio.create_task(service.run(), name="detection")
        listener_task = asyncio.create_task(listener.run(), name="detection-notify")
        stats_task = asyncio.create_task(
            run_stats_ticker(hub, stats),
            name="detection-stats",
        )
        app.state.service = service
        app.state.consumer_task = consumer_task
        app.state.listener_task = listener_task
        app.state.stats_task = stats_task
        try:
            yield
        finally:
            listener.stop()
            for task in (stats_task, listener_task, consumer_task):
                task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(
                    stats_task,
                    listener_task,
                    consumer_task,
                    return_exceptions=True,
                )
    finally:
        if service is not None:
            await service.aclose()
        await pool.close()
        await engine.dispose()


def create_app(
    settings: Settings | None = None,
    *,
    metrics: DetectionMetrics | None = None,
    include_test_routes: bool = False,
) -> FastAPI:
    """Build the detection API. Importing this module does not open connections."""
    application = FastAPI(title="detection", lifespan=_lifespan)
    application.state.settings = settings or Settings.from_env()
    application.state.metrics = METRICS if metrics is None else metrics
    # Defaults for unit tests that never enter lifespan.
    if not hasattr(application.state, "broadcast"):
        application.state.broadcast = BroadcastHub()
    if not hasattr(application.state, "stats"):
        application.state.stats = LiveStats()
    install_request_id_middleware(application)
    install_error_handlers(application)

    @application.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/ready")
    async def ready(request: Request) -> JSONResponse:
        task = request.app.state.consumer_task
        consumer = request.app.state.service
        if task.done():
            kafka_ok = False
        else:
            kafka_ok = await consumer.ready()
        postgres_ok = await _postgres_ready(request.app.state.engine)
        body = {
            "status": "ready" if kafka_ok and postgres_ok else "not_ready",
            "kafka": "ok" if kafka_ok else "down",
            "postgres": "ok" if postgres_ok else "down",
        }
        code = 200 if kafka_ok and postgres_ok else 503
        return JSONResponse(status_code=code, content=body)

    application.include_router(alerts_router)
    application.include_router(rules_router)
    application.include_router(stream_router)

    if include_test_routes:

        @application.get("/__test__/boom")
        async def boom() -> None:
            raise RuntimeError("boom pan 4111111111111111")

        @application.get("/__test__/probe")
        async def probe(value: int) -> dict[str, int]:
            return {"value": value}

    return application


def export_openapi(path: str) -> None:
    """Write the OpenAPI document. This does not connect to Kafka or Postgres."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(app.openapi(), indent=2) + "\n", encoding="utf-8")


app = create_app()
