"""Edge routing, API-key auth, and rate limiting.

``python -m gateway.api`` listens on port 8080. Importing this module does not
open connections.
"""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID, uuid4

import httpx
import uvicorn
from fastapi import FastAPI, Query
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import Response

from gateway.catalog import CatalogUnavailable, read_catalog
from gateway.logging import configure_logging
from gateway.models import AlertOut, AlertUpdate, CatalogEntry, ErrorBody, ReadyBody, Status
from gateway.proxy import forward, new_client
from gateway.settings import Settings
from schemas import FeatureRequest, FeatureResponse

_PORT = 8080
_READY_BUDGET_S = 1.0
_RATE_SCOPE = "client"
_OPEN_PATHS = frozenset(
    {
        "/health",
        "/ready",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
        "/openapi.json",
    }
)
_BUSINESS_PATHS = (
    "/v1/features",
    "/v1/alerts",
    "/v1/alerts/{alert_id}",
    "/v1/catalog",
)
_METHODS = frozenset({"get", "post", "patch", "put", "delete", "head", "options"})
_GATEWAY_ERRORS = {
    "401": "Missing API key",
    "403": "Unknown API key",
    "429": "Rate limit exceeded",
    "503": "Upstream or catalog unavailable",
}


def client_key(request: Request) -> str:
    """Rate-limit identity. The auth middleware sets the client name."""
    client = getattr(request.state, "client", None)
    if isinstance(client, str) and client:
        return client
    return "anonymous"


def _authenticate(request: Request) -> str | JSONResponse:
    settings: Settings = request.app.state.settings
    presented = request.headers.get("x-api-key")
    if presented is None or presented.strip() == "":
        return JSONResponse(status_code=401, content={"detail": "missing api key"})
    client = settings.api_keys.get(presented)
    if client is None:
        return JSONResponse(status_code=403, content={"detail": "unknown api key"})
    return client


def _log_request(
    request: Request,
    request_id: str,
    client: str | None,
    status: int,
    started: float,
) -> None:
    extra: dict[str, object] = {
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
        "status": status,
        "latency_ms": int((time.perf_counter() - started) * 1000),
    }
    if client is not None:
        extra["client"] = client
    logging.getLogger("gateway").info("request", extra=extra)


def _request_id_from(request: Request) -> str:
    value = getattr(request.state, "request_id", "")
    if isinstance(value, str) and value:
        return value
    return ""


def _install_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        started = time.perf_counter()
        incoming = request.headers.get("x-request-id", "").strip()
        request_id = incoming or str(uuid4())
        request.state.request_id = request_id
        client: str | None = None
        if request.url.path not in _OPEN_PATHS:
            authenticated = _authenticate(request)
            if isinstance(authenticated, JSONResponse):
                authenticated.headers["X-Request-ID"] = request_id
                _log_request(request, request_id, None, authenticated.status_code, started)
                return authenticated
            client = authenticated
            request.state.client = client
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        _log_request(request, request_id, client, response.status_code, started)
        return response


def _rate_limit_exceeded(request: Request, _exc: Exception) -> JSONResponse:
    response = JSONResponse(status_code=429, content={"detail": "rate limit exceeded"})
    current = getattr(request.state, "view_rate_limit", None)
    limiter = getattr(request.app.state, "limiter", None)
    if current is not None and limiter is not None:
        try:
            updated = limiter._inject_headers(response, current)
        except Exception:  # noqa: BLE001
            logging.getLogger("gateway").warning("rate limit headers failed", exc_info=True)
        else:
            if isinstance(updated, JSONResponse):
                response = updated
    if "retry-after" not in response.headers:
        response.headers["Retry-After"] = "1"
    request_id = _request_id_from(request)
    if request_id:
        response.headers["X-Request-ID"] = request_id
    return response


async def _probe(
    client: httpx.AsyncClient,
    name: str,
    base_url: str,
    budget: float,
) -> tuple[str, str]:
    url = f"{base_url.rstrip('/')}/health"
    try:
        async with asyncio.timeout(budget):
            response = await client.get(url, timeout=httpx.Timeout(budget))
    except (httpx.HTTPError, TimeoutError, OSError):
        logging.getLogger("gateway").warning("%s health failed", name)
        return name, "down"
    if response.status_code == 200:
        return name, "ok"
    return name, "down"


async def _ready(request: Request) -> JSONResponse:
    client: httpx.AsyncClient = request.app.state.client
    settings: Settings = request.app.state.settings
    pairs = await asyncio.gather(
        _probe(client, "features", settings.features_url, _READY_BUDGET_S),
        _probe(client, "detection", settings.detection_url, _READY_BUDGET_S),
    )
    status = dict(pairs)
    ready_ok = status["features"] == "ok" and status["detection"] == "ok"
    body = {
        "status": "ready" if ready_ok else "not_ready",
        "features": status["features"],
        "detection": status["detection"],
    }
    return JSONResponse(status_code=200 if ready_ok else 503, content=body)


async def _proxy(request: Request, base_url: str, service: str) -> Response:
    client: httpx.AsyncClient = request.app.state.client
    return await forward(client, request, base_url=base_url, service=service)


async def _catalog(request: Request) -> Response:
    try:
        rows = await read_catalog(request.app.state.pool)
    except CatalogUnavailable:
        return JSONResponse(status_code=503, content={"detail": "catalog unavailable"})
    return JSONResponse(content=rows)


def _install_routes(app: FastAPI, limiter: Limiter, rate: str) -> None:
    limited = limiter.shared_limit(rate, scope=_RATE_SCOPE, error_message="rate limit exceeded")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/ready",
        responses={200: {"model": ReadyBody}, 503: {"model": ReadyBody}},
    )
    async def ready(request: Request) -> JSONResponse:
        return await _ready(request)

    @app.post("/v1/features")
    @limited
    async def post_features(request: Request) -> Response:
        settings: Settings = request.app.state.settings
        return await _proxy(request, settings.features_url, "features")

    @app.get("/v1/alerts")
    @limited
    async def list_alerts(request: Request) -> Response:
        settings: Settings = request.app.state.settings
        return await _proxy(request, settings.detection_url, "detection")

    @app.patch("/v1/alerts/{alert_id}")
    @limited
    async def patch_alert(request: Request, alert_id: str) -> Response:
        # The id stays in the path. Detection validates it.
        _ = alert_id
        settings: Settings = request.app.state.settings
        return await _proxy(request, settings.detection_url, "detection")

    @app.get("/v1/catalog")
    @limited
    async def get_catalog(request: Request) -> Response:
        return await _catalog(request)


def _contract_app() -> FastAPI:
    """Routes used only to build the OpenAPI document."""
    contract = FastAPI(title="gateway", version="0.1.0")

    @contract.post("/v1/features", response_model=FeatureResponse)
    def post_features(body: FeatureRequest) -> FeatureResponse:
        raise RuntimeError(body.card_token)

    @contract.get("/v1/alerts", response_model=list[AlertOut])
    def list_alerts(
        status: Annotated[Status, Query()],
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> list[AlertOut]:
        raise RuntimeError(status, limit)

    @contract.patch(
        "/v1/alerts/{alert_id}",
        response_model=AlertOut,
        responses={
            404: {"description": "Unknown alert"},
            409: {"description": "Invalid transition"},
        },
    )
    def patch_alert(alert_id: UUID, body: AlertUpdate) -> AlertOut:
        raise RuntimeError(alert_id, body.status)

    @contract.get("/v1/catalog", response_model=list[CatalogEntry])
    def get_catalog() -> list[CatalogEntry]:
        raise RuntimeError("openapi only")

    return contract


def _error_response(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "content": {
            "application/json": {"schema": {"$ref": "#/components/schemas/ErrorBody"}},
        },
    }


def _apply_contract(schema: dict[str, Any]) -> None:
    contract = get_openapi(title="gateway", version="0.1.0", routes=_contract_app().routes)
    paths = schema.setdefault("paths", {})
    contract_paths = contract.get("paths", {})
    for path in _BUSINESS_PATHS:
        paths[path] = contract_paths[path]
    components = schema.setdefault("components", {})
    models = components.setdefault("schemas", {})
    models.update(contract.get("components", {}).get("schemas", {}))
    models["ErrorBody"] = ErrorBody.model_json_schema()
    components["securitySchemes"] = {
        "ApiKey": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "Client API key. The client name is the rate-limit identity.",
        }
    }
    for path in _BUSINESS_PATHS:
        path_item = paths[path]
        for method, operation in path_item.items():
            if method not in _METHODS or not isinstance(operation, dict):
                continue
            operation["security"] = [{"ApiKey": []}]
            responses = operation.setdefault("responses", {})
            for code, description in _GATEWAY_ERRORS.items():
                responses[code] = _error_response(description)
            parameters = operation.setdefault("parameters", [])
            if not any(
                isinstance(item, dict) and item.get("name") == "X-Request-ID" for item in parameters
            ):
                parameters.append(
                    {
                        "name": "X-Request-ID",
                        "in": "header",
                        "required": False,
                        "schema": {"type": "string"},
                        "description": "Correlation id. Generated when the header is empty.",
                    }
                )


def _bind_openapi(app: FastAPI) -> None:
    def openapi() -> dict[str, Any]:
        cached = app.openapi_schema
        if isinstance(cached, dict):
            return cached
        schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
        _apply_contract(schema)
        app.openapi_schema = schema
        return schema

    app.openapi = openapi  # type: ignore[method-assign]


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    configure_logging()
    client = new_client()
    pool: AsyncConnectionPool[AsyncConnection[TupleRow]] = AsyncConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=4,
        open=False,
    )
    try:
        await pool.open(wait=True, timeout=30)
        app.state.client = client
        app.state.pool = pool
        try:
            yield
        finally:
            await pool.close()
    finally:
        await client.aclose()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the gateway. Importing this module does not open connections."""
    resolved = settings if settings is not None else Settings.from_env()
    app = FastAPI(title="gateway", version="0.1.0", lifespan=_lifespan)
    app.state.settings = resolved
    configure_logging()
    limiter = Limiter(
        key_func=client_key,
        default_limits=[],
        headers_enabled=True,
        storage_uri="memory://",
        key_prefix=str(uuid4()),
        swallow_errors=False,
    )
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded)
    _install_middleware(app)
    _install_routes(app, limiter, resolved.rate_limit)
    _bind_openapi(app)
    return app


def export_openapi(path: str) -> None:
    """Write the OpenAPI document. This does not connect to Postgres or upstreams."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(app.openapi(), indent=2) + "\n", encoding="utf-8")


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=_PORT)  # noqa: S104


app = create_app()


if __name__ == "__main__":
    main()
