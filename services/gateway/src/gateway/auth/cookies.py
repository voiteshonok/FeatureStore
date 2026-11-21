"""Session cookie helpers (ADR-020)."""

from __future__ import annotations

from starlette.requests import Request
from starlette.responses import Response

from gateway.auth.jwt import SessionClaims, needs_renewal

COOKIE_NAME = "fs_session"
COOKIE_PATH = "/api"
_MAX_AGE_S = 60 * 60


def read_session_token(request: Request) -> str | None:
    """Return the raw JWT from the session cookie, if present."""
    token = request.cookies.get(COOKIE_NAME)
    if token is None or not token.strip():
        return None
    return token.strip()


def set_session_cookie(response: Response, token: str, *, secure: bool) -> None:
    """Attach ``fs_session`` with the ADR-020 cookie attributes."""
    response.set_cookie(
        key=COOKIE_NAME,
        value=token,
        max_age=_MAX_AGE_S,
        path=COOKIE_PATH,
        httponly=True,
        samesite="strict",
        secure=secure,
    )


def clear_session_cookie(response: Response, *, secure: bool) -> None:
    """Expire the session cookie."""
    response.delete_cookie(
        key=COOKIE_NAME,
        path=COOKIE_PATH,
        httponly=True,
        samesite="strict",
        secure=secure,
    )


def should_renew(claims: SessionClaims) -> bool:
    """Whether the gateway should re-issue the cookie on this response."""
    return needs_renewal(claims)
