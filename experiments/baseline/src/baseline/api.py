"""HTTP API for the SQL stand. ``python -m baseline.api`` listens on port 8003."""

import asyncio
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection, Error
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from baseline.metrics import API_METRICS, ApiMetrics, start_metrics_server
from baseline.settings import Settings
from baseline.values import (
    CardAggregates,
    GeoPoint,
    MerchantAggregates,
    assemble,
)
from baseline.windows import card_bounds, merchant_bounds
from schemas import FeatureRequest, FeatureResponse, FeatureValue, by_name

_READY_TIMEOUT_S = 1.0
_PORT = 8003
_WARM_AT = datetime(2000, 1, 1, tzinfo=UTC)

_CARD_SQL = """
SELECT
    (count(*) FILTER (WHERE event_time > %s))::bigint,
    (count(*) FILTER (WHERE event_time > %s))::bigint,
    count(*)::bigint,
    (coalesce(sum(amount_minor) FILTER (WHERE event_time > %s), 0))::bigint,
    (coalesce(sum(amount_minor) FILTER (WHERE event_time > %s), 0))::bigint,
    (coalesce(sum(amount_minor), 0))::bigint
FROM baseline.transactions
WHERE card_token = %s
  AND event_time > %s
  AND event_time <= %s
"""

_MERCHANT_SQL = """
SELECT
    count(*)::bigint,
    (count(*) FILTER (WHERE status = 'declined'))::bigint
FROM baseline.transactions
WHERE merchant_id = %s
  AND event_time >= %s
  AND event_time < %s
"""

_GEO_SQL = """
SELECT lat, lon, event_time
FROM baseline.transactions
WHERE card_token = %s
  AND lat IS NOT NULL
  AND lon IS NOT NULL
ORDER BY event_time DESC
LIMIT 1
"""


def _wall_now() -> datetime:
    return datetime.now(UTC)


def _window(name: str) -> timedelta:
    window = by_name(name).window
    if window is None:
        msg = f"{name} has no window"
        raise ValueError(msg)
    return window


def _as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        msg = "aggregate was not an integer"
        raise TypeError(msg)
    return value


async def fetch_card(
    conn: AsyncConnection[TupleRow],
    card_token: str,
    t: datetime,
) -> CardAggregates:
    """Count and sum one card over the three catalog windows."""
    lower_1m, _ = card_bounds(t, _window("card_cnt_1m"))
    lower_1h, _ = card_bounds(t, _window("card_cnt_1h"))
    lower_24h, end = card_bounds(t, _window("card_cnt_24h"))
    async with conn.cursor() as cur:
        await cur.execute(
            _CARD_SQL,
            (lower_1m, lower_1h, lower_1m, lower_1h, card_token, lower_24h, end),
        )
        row = await cur.fetchone()
    if row is None:
        msg = "card aggregate returned no row"
        raise RuntimeError(msg)
    return CardAggregates(
        cnt_1m=_as_int(row[0]),
        cnt_1h=_as_int(row[1]),
        cnt_24h=_as_int(row[2]),
        sum_1m=_as_int(row[3]),
        sum_1h=_as_int(row[4]),
        sum_24h=_as_int(row[5]),
    )


async def fetch_merchant(
    conn: AsyncConnection[TupleRow],
    merchant_id: str,
    t: datetime,
) -> MerchantAggregates:
    """Count and declined count over the merchant's 1,440 minute buckets."""
    start, end = merchant_bounds(t)
    async with conn.cursor() as cur:
        await cur.execute(_MERCHANT_SQL, (merchant_id, start, end))
        row = await cur.fetchone()
    if row is None:
        msg = "merchant aggregate returned no row"
        raise RuntimeError(msg)
    return MerchantAggregates(count=_as_int(row[0]), declined=_as_int(row[1]))


async def fetch_geo(conn: AsyncConnection[TupleRow], card_token: str) -> GeoPoint | None:
    """Latest card point that has coordinates, ordered by ``event_time``."""
    async with conn.cursor() as cur:
        await cur.execute(_GEO_SQL, (card_token,))
        row = await cur.fetchone()
    if row is None:
        return None
    lat, lon, event_time = row
    if not isinstance(lat, float | int) or isinstance(lat, bool):
        msg = "geo latitude was not a number"
        raise TypeError(msg)
    if not isinstance(lon, float | int) or isinstance(lon, bool):
        msg = "geo longitude was not a number"
        raise TypeError(msg)
    if not isinstance(event_time, datetime):
        msg = "geo event_time was not a timestamp"
        raise TypeError(msg)
    return GeoPoint(lat=float(lat), lon=float(lon), event_time=event_time)


async def warm(conn: AsyncConnection[TupleRow]) -> None:
    """Execute the three reads once so the connection prepares them."""
    await fetch_card(conn, "", _WARM_AT)
    await fetch_merchant(conn, "", _WARM_AT)
    await fetch_geo(conn, "")


def _pool(settings: Settings) -> AsyncConnectionPool[AsyncConnection[TupleRow]]:
    return AsyncConnectionPool(
        settings.database_url,
        min_size=settings.pool_min_size,
        max_size=settings.pool_max_size,
        kwargs={"autocommit": True, "prepare_threshold": 0},
        configure=warm,
        open=False,
    )


async def load_features(
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    request: FeatureRequest,
    now: datetime,
) -> dict[str, FeatureValue]:
    async with pool.connection() as conn:
        card = await fetch_card(conn, request.card_token, now)
        merchant = await fetch_merchant(conn, request.merchant_id, now)
        point = await fetch_geo(conn, request.card_token)
    return assemble(card, merchant, point, request, now)


async def _postgres_ready(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> bool:
    try:
        await asyncio.wait_for(_probe_postgres(pool), timeout=_READY_TIMEOUT_S)
    except Exception:
        return False
    return True


async def _probe_postgres(pool: AsyncConnectionPool[AsyncConnection[TupleRow]]) -> None:
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    start_metrics_server(settings.metrics_port, app.state.metrics.registry)
    pool = _pool(settings)
    await pool.open(wait=True, timeout=30)
    app.state.pool = pool
    try:
        yield
    finally:
        await pool.close()


def create_app(
    settings: Settings | None = None,
    *,
    metrics: ApiMetrics | None = None,
    now: Callable[[], datetime] | None = None,
) -> FastAPI:
    """Build the baseline API. Importing this module does not open connections."""
    app = FastAPI(title="baseline", lifespan=_lifespan)
    app.state.settings = settings or Settings.from_env()
    app.state.metrics = API_METRICS if metrics is None else metrics
    app.state.now = _wall_now if now is None else now

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready(request: Request) -> JSONResponse:
        postgres_ok = await _postgres_ready(request.app.state.pool)
        body = {
            "status": "ready" if postgres_ok else "not_ready",
            "postgres": "ok" if postgres_ok else "down",
        }
        return JSONResponse(status_code=200 if postgres_ok else 503, content=body)

    @app.post("/v1/features", response_model=FeatureResponse)
    async def post_features(body: FeatureRequest, request: Request) -> FeatureResponse:
        started = time.perf_counter()
        metrics: ApiMetrics = request.app.state.metrics
        try:
            features = await load_features(
                request.app.state.pool,
                body,
                request.app.state.now(),
            )
        except (Error, PoolTimeout, OSError):
            metrics.request_duration.labels(outcome="error").observe(time.perf_counter() - started)
            raise HTTPException(status_code=503, detail="postgres unavailable") from None
        metrics.request_duration.labels(outcome="ok").observe(time.perf_counter() - started)
        return FeatureResponse(request_id=uuid4(), features=features)

    return app


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=_PORT)  # noqa: S104


app = create_app()


if __name__ == "__main__":
    main()
