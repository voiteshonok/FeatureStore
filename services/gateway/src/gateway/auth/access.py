"""Dual auth, RBAC, and CSRF checks (ADR-020)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from starlette.requests import Request

from gateway.auth.cookies import read_session_token
from gateway.auth.jwt import SessionClaims, verify_token

AuthKind = Literal["api_key", "session"]

_MUTATING = frozenset({"POST", "PATCH", "PUT", "DELETE"})
_CSRF_HEADER = "x-requested-with"
_CSRF_VALUE = "fetch"

# Route templates allowed per auth kind / role.
_FEATURES = ("POST", "/v1/features")
_OPERATOR_GET = frozenset(
    {
        ("GET", "/v1/alerts"),
        ("GET", "/v1/alerts/{alert_id}"),
        ("GET", "/v1/rules"),
        ("GET", "/v1/catalog"),
        ("GET", "/v1/stream/alerts"),
        ("GET", "/v1/stream/system"),
    }
)
_AUTH_SESSION = frozenset(
    {
        ("POST", "/v1/auth/logout"),
        ("GET", "/v1/auth/me"),
    }
)


@dataclass(frozen=True)
class AuthContext:
    """Resolved caller for the current request."""

    kind: AuthKind
    client: str
    operator_id: str | None = None
    role: str | None = None
    claims: SessionClaims | None = None


def authenticate_request(request: Request, secret: bytes) -> AuthContext | str:
    """Return auth context, or ``missing`` / ``unknown_key`` / ``invalid_session``."""
    presented = request.headers.get("x-api-key")
    if presented is not None and presented.strip() != "":
        settings_keys = request.app.state.settings.api_keys
        client = settings_keys.get(presented)
        if client is None:
            return "unknown_key"
        return AuthContext(kind="api_key", client=client)

    token = read_session_token(request)
    if token is None:
        return "missing"
    claims = verify_token(token, secret)
    if claims is None:
        return "invalid_session"
    return AuthContext(
        kind="session",
        client=f"operator:{claims.operator_id}",
        operator_id=claims.operator_id,
        role=claims.role,
        claims=claims,
    )


def csrf_ok(request: Request, auth: AuthContext) -> bool:
    """Cookie-authenticated mutating requests need ``X-Requested-With: fetch``."""
    if auth.kind != "session":
        return True
    if request.method not in _MUTATING:
        return True
    if request.url.path == "/v1/auth/login":
        return True
    return request.headers.get(_CSRF_HEADER, "").lower() == _CSRF_VALUE


def rbac_allowed(method: str, route: str, auth: AuthContext) -> bool:
    """Return whether ``auth`` may call ``method`` ``route`` (template path)."""
    pair = (method, route)
    if pair in _AUTH_SESSION:
        return auth.kind == "session"
    if pair == _FEATURES:
        return True
    if pair in _OPERATOR_GET:
        return auth.kind == "session"
    if pair == ("PATCH", "/v1/alerts/{alert_id}"):
        return auth.kind == "session" and auth.role in {"analyst", "admin"}
    return False
