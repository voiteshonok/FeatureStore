"""HTTP API for feature reads. ``python -m features.api`` listens on port 8001."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

import uvicorn
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import Body, FastAPI, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.exceptions import RedisError

from features.errors import install_error_handlers, install_request_id_middleware
from features.examples import (
    API_DESCRIPTION,
    ERROR_CODES,
    FEATURE_EXAMPLES,
    FEATURE_RESPONSE_EXAMPLE,
    FEATURES_DESCRIPTION,
    HEALTH_DESCRIPTION,
    HEALTH_EXAMPLE,
    OPENAPI_TAGS,
    READY_DESCRIPTION,
    READY_DOWN,
    READY_OK,
    apply_response_examples,
)
from features.freshness import serve
from features.metrics import API_METRICS, ApiMetrics, start_metrics_server
from features.settings import Settings
from schemas import CATALOG, AppProblem, FeatureRequest, FeatureResponse, FeatureSpec, mask_pans
from schemas.openapi_problems import finalize_documented_openapi

_READY_TIMEOUT_S = 1.0
_PORT = 8001
_LOG_FIELDS = ("request_id", "card_token", "merchant_id", "ts", "latency_ms")
_CATALOG_VERSION = 1

_UPSERT_DEFINITION = """
INSERT INTO features.feature_definitions (name, entity, kind, agg, default_value)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (name) DO UPDATE SET
    entity = EXCLUDED.entity,
    kind = EXCLUDED.kind,
    agg = EXCLUDED.agg,
    default_value = EXCLUDED.default_value
WHERE features.feature_definitions.entity IS DISTINCT FROM EXCLUDED.entity
   OR features.feature_definitions.kind IS DISTINCT FROM EXCLUDED.kind
   OR features.feature_definitions.agg IS DISTINCT FROM EXCLUDED.agg
   OR features.feature_definitions.default_value IS DISTINCT FROM EXCLUDED.default_value
"""

_UPSERT_VERSION = """
INSERT INTO features.feature_versions (name, version, window_ms, stale_after_ms)
VALUES (%s, %s, %s, %s)
ON CONFLICT (name, version) DO UPDATE SET
    window_ms = EXCLUDED.window_ms,
    stale_after_ms = EXCLUDED.stale_after_ms
WHERE features.feature_versions.window_ms IS DISTINCT FROM EXCLUDED.window_ms
   OR features.feature_versions.stale_after_ms IS DISTINCT FROM EXCLUDED.stale_after_ms
"""


def _mask_value(value: object) -> object:
    if isinstance(value, str):
        return mask_pans(value)
    return value


def _mask_args(
    args: tuple[object, ...] | Mapping[str, object] | None,
) -> tuple[object, ...] | Mapping[str, object] | None:
    if isinstance(args, tuple):
        return tuple(_mask_value(item) for item in args)
    if isinstance(args, dict):
        return {str(key): _mask_value(value) for key, value in args.items()}
    return args


class PanMaskFilter(logging.Filter):
    """Mask 13–19 digit runs in the log message and its arguments."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = mask_pans(record.msg)
        record.args = _mask_args(record.args)
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per record, with request fields when present."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": mask_pans(record.getMessage()),
        }
        for key in _LOG_FIELDS:
            if hasattr(record, key):
                payload[key] = _mask_value(getattr(record, key))
        if record.exc_info:
            payload["exception"] = mask_pans(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    """Attach the JSON formatter and the PAN filter once."""
    logger = logging.getLogger("features")
    if any(isinstance(handler.formatter, JsonFormatter) for handler in logger.handlers):
        return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(PanMaskFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _millis(value: timedelta | None) -> int | None:
    if value is None:
        return None
    return int(value.total_seconds() * 1000)


def _default_value(spec: FeatureSpec) -> float | None:
    if spec.default is None:
        return None
    return float(spec.default)


async def upsert_catalog(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
    """Insert the code catalog. A second call leaves identical rows in place."""
    async with pool.connection() as conn, conn.transaction():
        async with conn.cursor() as cur:
            for spec in CATALOG:
                await cur.execute(
                    _UPSERT_DEFINITION,
                    (spec.name, spec.entity, spec.kind, spec.agg, _default_value(spec)),
                )
                await cur.execute(
                    _UPSERT_VERSION,
                    (spec.name, _CATALOG_VERSION, _millis(spec.window), _millis(spec.stale_after)),
                )


def _partition_count(described: object, topic: str) -> int | None:
    if not isinstance(described, list):
        return None
    for item in described:
        if not isinstance(item, dict) or item.get("topic") != topic:
            continue
        if item.get("error_code") not in (0, None):
            return None
        partitions = item.get("partitions")
        if not isinstance(partitions, list):
            return None
        return len(partitions)
    return None


async def _close_admin(admin: AIOKafkaAdminClient, logger: logging.Logger) -> None:
    try:
        await admin.close()
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
    except Exception:
        logger.warning("kafka admin close failed", exc_info=True)


async def assert_topic_partitions(settings: Settings) -> None:
    """Wait until ``transactions.raw`` exists, then require ``n_partitions``."""
    logger = logging.getLogger("features")
    backoff = settings.redis_backoff_s
    while True:
        admin = AIOKafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap,
            client_id="features-api",
        )
        count: int | None = None
        try:
            await admin.start()
            described = await admin.describe_topics([settings.raw_topic])
            count = _partition_count(described, settings.raw_topic)
        except Exception:
            logger.warning("kafka metadata check failed", exc_info=True)
            count = None
        finally:
            await _close_admin(admin, logger)
        if count is None:
            logger.info("waiting for topic %s", settings.raw_topic)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, settings.redis_backoff_max_s)
            continue
        if count != settings.n_partitions:
            msg = f"{settings.raw_topic} has {count} partitions, expected {settings.n_partitions}"
            raise RuntimeError(msg)
        return


async def _redis_ready(redis: Redis) -> bool:
    try:
        await asyncio.wait_for(redis.ping(), timeout=_READY_TIMEOUT_S)
    except Exception:
        return False
    return True


async def _postgres_ready(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> bool:
    try:
        await asyncio.wait_for(_probe_postgres(pool), timeout=_READY_TIMEOUT_S)
    except Exception:
        return False
    return True


async def _probe_postgres(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


def _log_request(body: FeatureRequest, request_id: object, started: float) -> None:
    logging.getLogger("features").info(
        "served features",
        extra={
            "request_id": str(request_id),
            "card_token": body.card_token,
            "merchant_id": body.merchant_id,
            "ts": body.ts.isoformat(),
            "latency_ms": int((time.perf_counter() - started) * 1000),
        },
    )


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    start_metrics_server(settings.metrics_port, app.state.metrics.registry)
    configure_logging()
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
    )
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]] = AsyncConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
    )
    await pool.open(wait=True, timeout=30)
    try:
        await upsert_catalog(pool)
        await assert_topic_partitions(settings)
        app.state.redis = redis
        app.state.pool = pool
        yield
    finally:
        await pool.close()
        await redis.aclose()


def create_app(
    settings: Settings | None = None,
    *,
    metrics: ApiMetrics | None = None,
    include_test_routes: bool = False,
) -> FastAPI:
    """Build the feature API. Importing this module does not open connections."""
    app = FastAPI(
        title="Feature Store Features",
        version="1.0.0",
        description=API_DESCRIPTION,
        servers=[{"url": "http://localhost:8001"}],
        license_info={"name": "MIT", "identifier": "MIT"},
        openapi_tags=OPENAPI_TAGS,
        lifespan=_lifespan,
    )
    app.state.settings = settings or Settings.from_env()
    app.state.metrics = API_METRICS if metrics is None else metrics
    configure_logging()
    install_request_id_middleware(app)
    install_error_handlers(app)

    @app.get(
        "/health",
        tags=["ops"],
        summary="Health",
        description=HEALTH_DESCRIPTION,
        openapi_extra={"security": []},
        responses={
            200: {
                "description": "Process is running.",
                "content": {"application/json": {"example": HEALTH_EXAMPLE}},
            }
        },
    )
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/ready",
        tags=["ops"],
        summary="Ready",
        description=READY_DESCRIPTION,
        openapi_extra={"security": []},
        responses={
            200: {
                "description": "Redis and Postgres are up.",
                "content": {"application/json": {"example": READY_OK}},
            },
            503: {
                "description": "A dependency is down.",
                "content": {"application/json": {"example": READY_DOWN}},
            },
        },
    )
    async def ready(request: Request) -> JSONResponse:
        redis_ok = await _redis_ready(request.app.state.redis)
        postgres_ok = await _postgres_ready(request.app.state.pool)
        body = {
            "status": "ready" if redis_ok and postgres_ok else "not_ready",
            "redis": "ok" if redis_ok else "down",
            "postgres": "ok" if postgres_ok else "down",
        }
        return JSONResponse(status_code=200 if redis_ok and postgres_ok else 503, content=body)

    @app.post(
        "/v1/features",
        response_model=FeatureResponse,
        tags=["features"],
        summary="Read features",
        description=FEATURES_DESCRIPTION,
        openapi_extra={"security": []},
        responses={
            200: {
                "description": "Feature vector.",
                "content": {"application/json": {"example": FEATURE_RESPONSE_EXAMPLE}},
            }
        },
    )
    async def post_features(
        body: Annotated[FeatureRequest, Body(openapi_examples=FEATURE_EXAMPLES)],
        request: Request,
    ) -> FeatureResponse:
        started = time.perf_counter()
        request_id = uuid4()
        metrics: ApiMetrics = request.app.state.metrics
        try:
            features = await serve(
                request.app.state.redis,
                body,
                now=datetime.now(UTC),
                n_partitions=request.app.state.settings.n_partitions,
            )
        except RedisError:
            metrics.request_duration.labels(outcome="error").observe(time.perf_counter() - started)
            _log_request(body, request_id, started)
            raise AppProblem(
                "upstream_unavailable",
                "redis unavailable",
                503,
            ) from None
        for name, value in features.items():
            metrics.values.labels(feature=name, status=value.status.value).inc()
        metrics.request_duration.labels(outcome="ok").observe(time.perf_counter() - started)
        _log_request(body, request_id, started)
        return FeatureResponse(request_id=request_id, features=features)

    if include_test_routes:

        @app.get("/__test__/boom")
        async def boom() -> None:
            raise RuntimeError("boom pan 4111111111111111")

    _bind_openapi(app)
    return app


def _bind_openapi(app: FastAPI) -> None:
    def openapi() -> dict[str, Any]:
        cached = app.openapi_schema
        if isinstance(cached, dict):
            return cached
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
            tags=app.openapi_tags,
            servers=app.servers,
            license_info=app.license_info,
        )
        finalize_documented_openapi(
            schema,
            tags=OPENAPI_TAGS,
            errors=ERROR_CODES,
            public=True,
        )
        apply_response_examples(schema)
        app.openapi_schema = schema
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


def export_openapi(path: str) -> None:
    """Write the OpenAPI document. This does not connect to Redis or Postgres."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(app.openapi(), indent=2) + "\n", encoding="utf-8")


def main() -> None:
    import os

    workers = int(os.environ.get("FEATURES_API_WORKERS", "1"))
    # Import string is required when workers > 1 (each worker loads the app).
    if workers <= 1:
        uvicorn.run(app, host="0.0.0.0", port=_PORT)  # noqa: S104
        return
    uvicorn.run(
        "features.api:app",
        host="0.0.0.0",  # noqa: S104
        port=_PORT,
        workers=workers,
    )


app = create_app()


if __name__ == "__main__":
    main()
