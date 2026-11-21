"""Edge routing, dual auth, and rate limiting.

``python -m gateway.api`` listens on port 8080. Importing this module does not
open connections.
"""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal, cast
from uuid import UUID, uuid4

import httpx
import uvicorn
from fastapi import FastAPI, Query
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, Response
from psycopg import AsyncConnection
from psycopg.rows import TupleRow
from psycopg_pool import AsyncConnectionPool
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request
from starlette.responses import Response as StarletteResponse
from starlette.routing import Match

from gateway.auth.access import AuthContext, authenticate_request, csrf_ok, rbac_allowed
from gateway.auth.cookies import clear_session_cookie, set_session_cookie, should_renew
from gateway.auth.jwt import issue_token
from gateway.auth.operators import (
    OperatorsUnavailable,
    fetch_operator_by_email,
    fetch_operator_by_id,
)
from gateway.auth.password import passwords_match
from gateway.auth.secret import JwtSecretError, load_jwt_secret
from gateway.catalog import CatalogUnavailable, read_catalog
from gateway.errors import install_error_handlers, problem_response, unhandled_response
from gateway.logging import configure_logging
from gateway.metrics import METRICS, GatewayMetrics, start_metrics_server
from gateway.models import (
    AlertDetailOut,
    AlertListOut,
    AlertOut,
    AlertUpdate,
    CatalogEntry,
    LoginBody,
    MeOut,
    ProblemDetails,
    ReadyBody,
    RuleOut,
    Sort,
    Status,
)
from gateway.proxy import forward, forward_stream, new_client
from gateway.settings import Settings
from schemas import FeatureRequest, FeatureResponse

_PORT = 8080
_READY_BUDGET_S = 1.0
_RATE_SCOPE = "client"
_LOGIN_RATE = "5/minute"
_OPEN_PATHS = frozenset(
    {
        "/health",
        "/ready",
        "/docs",
        "/docs/oauth2-redirect",
        "/redoc",
        "/openapi.json",
        "/v1/auth/login",
    }
)
_BUSINESS_PATHS = (
    "/v1/features",
    "/v1/alerts",
    "/v1/alerts/{alert_id}",
    "/v1/rules",
    "/v1/catalog",
    "/v1/auth/login",
    "/v1/auth/logout",
    "/v1/auth/me",
)
_METHODS = frozenset({"get", "post", "patch", "put", "delete", "head", "options"})
_GATEWAY_ERRORS = {
    "401": "Missing credentials or invalid session",
    "403": "Forbidden",
    "429": "Rate limit exceeded",
    "503": "Upstream or catalog unavailable",
    "504": "Upstream timeout",
}


def client_key(request: Request) -> str:
    """Rate-limit identity. The auth middleware sets the client name."""
    client = getattr(request.state, "client", None)
    if isinstance(client, str) and client:
        return client
    return "anonymous"


def login_key(request: Request) -> str:
    """Rate-limit login attempts by client IP."""
    if request.client is not None and request.client.host:
        return request.client.host
    return "anonymous"


def _jwt_secret(request: Request) -> bytes:
    secret = getattr(request.app.state, "jwt_secret", None)
    if isinstance(secret, bytes) and secret:
        return secret
    msg = "jwt secret is not configured"
    raise RuntimeError(msg)


def _route_template(request: Request) -> str:
    """Route template. A raw path is never a label (ADR-014)."""
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    if isinstance(path, str):
        return path
    for candidate in request.app.router.routes:
        matches = getattr(candidate, "matches", None)
        if matches is None:
            continue
        match, _child = matches(request.scope)
        if match == Match.FULL:
            found = getattr(candidate, "path", None)
            if isinstance(found, str):
                return found
    return "unmatched"


def _observe_request(request: Request, status: int, started: float) -> None:
    metrics: GatewayMetrics = request.app.state.metrics
    metrics.request_duration.labels(
        route=_route_template(request),
        method=request.method,
        status=str(status),
    ).observe(time.perf_counter() - started)


def _log_request(
    request: Request,
    request_id: str,
    client: str | None,
    status: int,
    started: float,
    *,
    operator_id: str | None = None,
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
    if operator_id is not None:
        extra["operator_id"] = operator_id
    logging.getLogger("gateway").info("request", extra=extra)


def _request_id_from(request: Request) -> str:
    value = getattr(request.state, "request_id", "")
    if isinstance(value, str) and value:
        return value
    return ""


def _auth_error(request: Request, code: str) -> JSONResponse:
    if code == "unknown_key":
        return problem_response(
            request,
            type="forbidden",
            title="unknown api key",
            status=403,
        )
    if code == "invalid_session":
        return problem_response(
            request,
            type="unauthorized",
            title="invalid session",
            status=401,
        )
    return problem_response(
        request,
        type="unauthorized",
        title="missing credentials",
        status=401,
    )


def _apply_session_state(request: Request, auth: AuthContext) -> None:
    request.state.client = auth.client
    request.state.auth_kind = auth.kind
    if auth.operator_id is not None:
        request.state.operator_id = auth.operator_id
    if auth.role is not None:
        request.state.operator_role = auth.role
    if auth.claims is not None:
        request.state.session_claims = auth.claims


def _maybe_renew_cookie(request: Request, response: StarletteResponse) -> None:
    claims = getattr(request.state, "session_claims", None)
    if claims is None or not should_renew(claims):
        return
    settings: Settings = request.app.state.settings
    token = issue_token(
        operator_id=claims.operator_id,
        role=claims.role,
        secret=_jwt_secret(request),
    )
    set_session_cookie(response, token, secure=settings.cookie_secure)


def _install_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def correlation(
        request: Request,
        call_next: Callable[[Request], Awaitable[StarletteResponse]],
    ) -> StarletteResponse:
        started = time.perf_counter()
        incoming = request.headers.get("x-request-id", "").strip()
        request_id = incoming or str(uuid4())
        request.state.request_id = request_id
        client: str | None = None
        operator_id: str | None = None
        # ``/__test__/`` routes are only mounted in unit tests (include_test_routes).
        if request.url.path not in _OPEN_PATHS and not request.url.path.startswith("/__test__/"):
            try:
                secret = _jwt_secret(request)
            except RuntimeError:
                response = problem_response(
                    request,
                    type="internal_error",
                    title="Internal server error",
                    status=500,
                )
                response.headers["X-Request-ID"] = request_id
                return response
            authenticated = authenticate_request(request, secret)
            if isinstance(authenticated, str):
                error = _auth_error(request, authenticated)
                error.headers["X-Request-ID"] = request_id
                _log_request(request, request_id, None, error.status_code, started)
                _observe_request(request, error.status_code, started)
                return error
            if not csrf_ok(request, authenticated):
                error = problem_response(
                    request,
                    type="forbidden",
                    title="csrf header required",
                    status=403,
                )
                error.headers["X-Request-ID"] = request_id
                _log_request(
                    request,
                    request_id,
                    authenticated.client,
                    403,
                    started,
                    operator_id=authenticated.operator_id,
                )
                _observe_request(request, 403, started)
                return error
            route = _route_template(request)
            if not rbac_allowed(request.method, route, authenticated):
                error = problem_response(
                    request,
                    type="forbidden",
                    title="forbidden",
                    status=403,
                )
                error.headers["X-Request-ID"] = request_id
                _log_request(
                    request,
                    request_id,
                    authenticated.client,
                    403,
                    started,
                    operator_id=authenticated.operator_id,
                )
                _observe_request(request, 403, started)
                return error
            _apply_session_state(request, authenticated)
            client = authenticated.client
            operator_id = authenticated.operator_id
        try:
            result: StarletteResponse = await call_next(request)
        except Exception as exc:
            # BaseHTTPMiddleware can re-raise past ExceptionMiddleware.
            result = unhandled_response(request, exc)
        if getattr(request.state, "auth_kind", None) == "session":
            _maybe_renew_cookie(request, result)
        result.headers["X-Request-ID"] = request_id
        _log_request(
            request,
            request_id,
            client,
            result.status_code,
            started,
            operator_id=operator_id,
        )
        _observe_request(request, result.status_code, started)
        return result


def _rate_limit_exceeded(request: Request, _exc: Exception) -> JSONResponse:
    response = problem_response(
        request,
        type="rate_limited",
        title="rate limit exceeded",
        status=429,
    )
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
    metrics: GatewayMetrics = request.app.state.metrics
    client = getattr(request.state, "client", None)
    name = client if isinstance(client, str) and client else "anonymous"
    metrics.rate_limited.labels(client=name).inc()
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
        return problem_response(
            request,
            type="upstream_unavailable",
            title="catalog unavailable",
            status=503,
        )
    return JSONResponse(content=rows)


async def _login(request: Request, body: LoginBody) -> Response:
    settings: Settings = request.app.state.settings
    logger = logging.getLogger("gateway")
    email = body.email.strip().lower()
    try:
        operator = await fetch_operator_by_email(request.app.state.pool, email)
    except OperatorsUnavailable:
        return problem_response(
            request,
            type="upstream_unavailable",
            title="operators unavailable",
            status=503,
        )
    password_hash = operator.password_hash if operator is not None else None
    matched = passwords_match(body.password, password_hash)
    if operator is None or not matched:
        logger.info(
            "login",
            extra={
                "request_id": _request_id_from(request),
                "outcome": "failure",
                "path": request.url.path,
            },
        )
        return problem_response(
            request,
            type="unauthorized",
            title="invalid credentials",
            status=401,
        )
    operator_id = str(operator.operator_id)
    token = issue_token(
        operator_id=operator_id,
        role=operator.role,
        secret=_jwt_secret(request),
    )
    response = Response(status_code=204)
    set_session_cookie(response, token, secure=settings.cookie_secure)
    logger.info(
        "login",
        extra={
            "request_id": _request_id_from(request),
            "operator_id": operator_id,
            "outcome": "success",
            "path": request.url.path,
        },
    )
    return response


async def _logout(request: Request) -> Response:
    settings: Settings = request.app.state.settings
    response = Response(status_code=204)
    clear_session_cookie(response, secure=settings.cookie_secure)
    return response


async def _me(request: Request) -> Response:
    operator_id = getattr(request.state, "operator_id", None)
    if not isinstance(operator_id, str) or not operator_id:
        return problem_response(
            request,
            type="unauthorized",
            title="missing credentials",
            status=401,
        )
    try:
        operator = await fetch_operator_by_id(request.app.state.pool, operator_id)
    except OperatorsUnavailable:
        return problem_response(
            request,
            type="upstream_unavailable",
            title="operators unavailable",
            status=503,
        )
    if operator is None:
        return problem_response(
            request,
            type="unauthorized",
            title="invalid session",
            status=401,
        )
    if operator.role not in {"viewer", "analyst", "admin"}:
        return problem_response(
            request,
            type="unauthorized",
            title="invalid session",
            status=401,
        )
    role = cast(Literal["viewer", "analyst", "admin"], operator.role)
    body = MeOut(
        operator_id=str(operator.operator_id),
        email=operator.email,
        role=role,
    )
    return JSONResponse(content=body.model_dump())


def _install_routes(app: FastAPI, limiter: Limiter, rate: str) -> None:
    limited = limiter.shared_limit(rate, scope=_RATE_SCOPE, error_message="rate limit exceeded")
    login_limited = limiter.limit(
        _LOGIN_RATE,
        key_func=login_key,
        error_message="rate limit exceeded",
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/ready",
        responses={200: {"model": ReadyBody}, 503: {"model": ReadyBody}},
    )
    async def ready(request: Request) -> JSONResponse:
        return await _ready(request)

    @app.post("/v1/auth/login", status_code=204)
    @login_limited
    async def login(request: Request, body: LoginBody) -> Response:
        return await _login(request, body)

    @app.post("/v1/auth/logout", status_code=204)
    @limited
    async def logout(request: Request) -> Response:
        return await _logout(request)

    @app.get("/v1/auth/me", response_model=MeOut)
    @limited
    async def me(request: Request) -> Response:
        return await _me(request)

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

    @app.get("/v1/alerts/{alert_id}")
    @limited
    async def get_alert(request: Request, alert_id: str) -> Response:
        _ = alert_id
        settings: Settings = request.app.state.settings
        return await _proxy(request, settings.detection_url, "detection")

    @app.patch("/v1/alerts/{alert_id}")
    @limited
    async def patch_alert(request: Request, alert_id: str) -> Response:
        # The id stays in the path. Detection validates it.
        _ = alert_id
        settings: Settings = request.app.state.settings
        return await _proxy(request, settings.detection_url, "detection")

    @app.get("/v1/rules")
    @limited
    async def list_rules(request: Request) -> Response:
        settings: Settings = request.app.state.settings
        return await _proxy(request, settings.detection_url, "detection")

    @app.get("/v1/catalog")
    @limited
    async def get_catalog(request: Request) -> Response:
        return await _catalog(request)

    @app.get("/v1/stream/alerts")
    @limited
    async def stream_alerts(request: Request) -> Response:
        settings: Settings = request.app.state.settings
        close_after_s: float | None = None
        claims = getattr(request.state, "session_claims", None)
        if claims is not None and getattr(claims, "exp", None) is not None:
            remaining = (claims.exp - datetime.now(UTC)).total_seconds()
            close_after_s = max(0.0, remaining)
        return await forward_stream(
            request.app.state.client,
            request,
            base_url=settings.detection_url,
            service="detection",
            close_after_s=close_after_s,
        )


def _contract_app() -> FastAPI:
    """Routes used only to build the OpenAPI document."""
    contract = FastAPI(title="gateway", version="0.1.0")

    @contract.post("/v1/auth/login", status_code=204)
    def login(body: LoginBody) -> None:
        raise RuntimeError(body.email)

    @contract.post("/v1/auth/logout", status_code=204)
    def logout() -> None:
        raise RuntimeError("openapi only")

    @contract.get("/v1/auth/me", response_model=MeOut)
    def me() -> MeOut:
        raise RuntimeError("openapi only")

    @contract.post("/v1/features", response_model=FeatureResponse)
    def post_features(body: FeatureRequest) -> FeatureResponse:
        raise RuntimeError(body.card_token)

    @contract.get(
        "/v1/alerts",
        response_model=AlertListOut,
        responses={422: {"description": "Invalid cursor or query"}},
    )
    def list_alerts(
        status: Annotated[list[Status] | None, Query()] = None,
        entity: Annotated[str | None, Query()] = None,
        rule_id: Annotated[UUID | None, Query()] = None,
        created_from: Annotated[str | None, Query()] = None,
        created_to: Annotated[str | None, Query()] = None,
        sort: Annotated[Sort, Query()] = "-created_at",
        cursor: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> AlertListOut:
        raise RuntimeError(status, entity, rule_id, created_from, created_to, sort, cursor, limit)

    @contract.get(
        "/v1/alerts/{alert_id}",
        response_model=AlertDetailOut,
        responses={404: {"description": "Unknown alert"}},
    )
    def get_alert(alert_id: UUID) -> AlertDetailOut:
        raise RuntimeError(alert_id)

    @contract.patch(
        "/v1/alerts/{alert_id}",
        response_model=AlertOut,
        responses={
            403: {"description": "Viewer cannot change status"},
            404: {"description": "Unknown alert"},
            409: {"description": "Invalid transition"},
            422: {"description": "Missing or invalid note / headers"},
        },
    )
    def patch_alert(alert_id: UUID, body: AlertUpdate) -> AlertOut:
        raise RuntimeError(alert_id, body.status)

    @contract.get("/v1/rules", response_model=list[RuleOut])
    def list_rules() -> list[RuleOut]:
        raise RuntimeError("openapi only")

    @contract.get("/v1/catalog", response_model=list[CatalogEntry])
    def get_catalog() -> list[CatalogEntry]:
        raise RuntimeError("openapi only")

    return contract


def _error_response(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/ProblemDetails"},
            },
        },
    }


def _apply_contract(schema: dict[str, Any]) -> None:
    contract = get_openapi(title="gateway", version="0.1.0", routes=_contract_app().routes)
    paths = schema.setdefault("paths", {})
    contract_paths = contract.get("paths", {})
    for path in _BUSINESS_PATHS:
        if path in contract_paths:
            paths[path] = contract_paths[path]
    components = schema.setdefault("components", {})
    models = components.setdefault("schemas", {})
    models.update(contract.get("components", {}).get("schemas", {}))
    models["ProblemDetails"] = ProblemDetails.model_json_schema()
    components["securitySchemes"] = {
        "ApiKey": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "Machine client API key. The client name is the rate-limit identity.",
        },
        "SessionCookie": {
            "type": "apiKey",
            "in": "cookie",
            "name": "fs_session",
            "description": "Operator JWT session cookie (ADR-020).",
        },
    }
    session_only = {"/v1/auth/logout", "/v1/auth/me"}
    either = {
        "/v1/features",
        "/v1/alerts",
        "/v1/alerts/{alert_id}",
        "/v1/rules",
        "/v1/catalog",
    }
    for path in _BUSINESS_PATHS:
        path_item = paths.get(path)
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in _METHODS or not isinstance(operation, dict):
                continue
            if path == "/v1/auth/login":
                operation.pop("security", None)
            elif path in session_only:
                operation["security"] = [{"SessionCookie": []}]
            elif path in either:
                operation["security"] = [{"ApiKey": []}, {"SessionCookie": []}]
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


def _resolve_jwt_secret(settings: Settings) -> bytes:
    if settings.jwt_secret is not None:
        return settings.jwt_secret
    return load_jwt_secret(settings.jwt_secret_path)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    start_metrics_server(settings.metrics_port, app.state.metrics.registry)
    configure_logging()
    if not isinstance(getattr(app.state, "jwt_secret", None), bytes):
        try:
            app.state.jwt_secret = _resolve_jwt_secret(settings)
        except JwtSecretError:
            logging.getLogger("gateway").exception("jwt secret missing")
            raise
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


def create_app(
    settings: Settings | None = None,
    *,
    metrics: GatewayMetrics | None = None,
    include_test_routes: bool = False,
) -> FastAPI:
    """Build the gateway. Importing this module does not open connections."""
    resolved = settings if settings is not None else Settings.from_env()
    app = FastAPI(title="gateway", version="0.1.0", lifespan=_lifespan)
    app.state.settings = resolved
    app.state.metrics = METRICS if metrics is None else metrics
    if resolved.jwt_secret is not None:
        app.state.jwt_secret = resolved.jwt_secret
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
    install_error_handlers(app)
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded)
    _install_middleware(app)
    _install_routes(app, limiter, resolved.rate_limit)
    if include_test_routes:

        @app.get("/__test__/boom")
        async def boom() -> None:
            raise RuntimeError("boom pan 4111111111111111")

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
