"""HTTP entry point for payment events."""

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel

from ingestion.idempotency import AcceptResult, IdempotencyStore
from ingestion.logging import JsonFormatter, PanMaskFilter, mask_pans
from ingestion.publisher import KafkaPublisher, PublishError
from ingestion.tokenize import load_key, to_event
from schemas import PaymentIn, check_event_time

_READY_TIMEOUT_S = 1.0
_DELETE_EXPIRED = "SELECT ingestion.delete_expired_idempotency_keys()"
_DEFAULT_DATABASE_URL = "postgresql://featurestore:featurestore@localhost:5432/featurestore"
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class Settings:
    """Process settings taken from the environment."""

    kafka_bootstrap: str = "localhost:29092"
    database_url: str = _DEFAULT_DATABASE_URL
    hmac_key_path: str | None = None
    cleanup_interval_s: float = 600.0
    ack_timeout_s: float = 10.0

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092"),
            database_url=os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL),
            hmac_key_path=os.environ.get("HMAC_KEY_FILE"),
            cleanup_interval_s=float(os.environ.get("CLEANUP_INTERVAL_S", "600")),
            ack_timeout_s=float(os.environ.get("KAFKA_ACK_TIMEOUT_S", "10")),
        )


class EventAccepted(BaseModel):
    """Body returned when an event is accepted or recognized as a duplicate."""

    event_id: UUID
    duplicate: bool


def _configure_logging() -> None:
    logger = logging.getLogger("ingestion")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(PanMaskFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _field_error(field: str, message: str) -> list[dict[str, object]]:
    return [{"loc": ["body", field], "msg": mask_pans(message), "type": "value_error"}]


def _validation_error(_request: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, RequestValidationError):
        raise exc
    detail = [
        {
            "loc": list(err.get("loc", ())),
            "msg": mask_pans(str(err.get("msg", ""))),
            "type": str(err.get("type", "value_error")),
        }
        for err in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"detail": detail})


async def _postgres_ready(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> bool:
    try:
        await asyncio.wait_for(_probe_postgres(pool), timeout=_READY_TIMEOUT_S)
    except Exception:
        return False
    return True


async def _probe_postgres(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


async def _delete_expired(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
    async with pool.connection() as conn:
        await conn.execute(_DELETE_EXPIRED)


async def _cleanup_loop(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    interval: float,
    sleep: Sleep = asyncio.sleep,
) -> None:
    logger = logging.getLogger("ingestion")
    while True:
        await sleep(interval)
        try:
            await _delete_expired(pool)
        except Exception:
            logger.exception("idempotency cleanup failed")


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    _configure_logging()
    app.state.hmac_key = load_key(settings.hmac_key_path)
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]] = AsyncConnectionPool(
        settings.database_url,
        min_size=2,
        max_size=10,
        open=False,
    )
    publisher = KafkaPublisher(settings.kafka_bootstrap, ack_timeout_s=settings.ack_timeout_s)
    await pool.open(wait=True, timeout=30)
    try:
        await publisher.start()
        app.state.pool = pool
        app.state.publisher = publisher
        app.state.store = IdempotencyStore(pool)
        cleanup = asyncio.create_task(_cleanup_loop(pool, settings.cleanup_interval_s))
        try:
            yield
        finally:
            cleanup.cancel()
            with suppress(asyncio.CancelledError):
                await cleanup
            await publisher.aclose()
    finally:
        await pool.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the ingestion API. Importing this module does not open connections."""
    app = FastAPI(title="ingestion", lifespan=_lifespan)
    app.state.settings = settings or Settings.from_env()
    app.add_exception_handler(RequestValidationError, _validation_error)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(request: Request) -> JSONResponse:
        postgres_ok = await _postgres_ready(request.app.state.pool)
        kafka_ok = await request.app.state.publisher.ready()
        if postgres_ok and kafka_ok:
            return JSONResponse(status_code=200, content={"status": "ready"})
        return JSONResponse(status_code=503, content={"status": "not_ready"})

    @app.post("/v1/events", status_code=202, response_model=EventAccepted)
    async def post_event(payment: PaymentIn, request: Request) -> EventAccepted:
        reason = check_event_time(payment.event_time, datetime.now(UTC))
        if reason is not None:
            raise HTTPException(status_code=422, detail=_field_error("event_time", reason))
        event = to_event(payment, request.app.state.hmac_key, datetime.now(UTC))
        try:
            result = await request.app.state.store.accept(
                event,
                request.app.state.publisher.publish,
            )
        except PublishError:
            raise HTTPException(
                status_code=503,
                detail="event bus unavailable",
                headers={"Retry-After": "1"},
            ) from None
        return EventAccepted(
            event_id=event.event_id,
            duplicate=result is AcceptResult.DUPLICATE,
        )

    return app


def export_openapi(path: str) -> None:
    """Write the OpenAPI document. This does not connect to Kafka or Postgres."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(app.openapi(), indent=2) + "\n", encoding="utf-8")


app = create_app()
