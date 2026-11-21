"""Issue and verify operator session JWTs (ADR-020)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

import jwt

_ALGORITHM = "HS256"
_TTL = timedelta(hours=1)
_RENEW_WITHIN = timedelta(minutes=15)
_DECODE_OPTIONS: dict[str, Any] = {
    "require": ["sub", "role", "iat", "exp", "jti"],
    "verify_signature": True,
    "verify_exp": True,
    "verify_iat": True,
}


@dataclass(frozen=True)
class SessionClaims:
    """Verified session payload."""

    operator_id: str
    role: str
    exp: datetime
    jti: str


def issue_token(
    *,
    operator_id: str,
    role: str,
    secret: bytes,
    now: datetime | None = None,
) -> str:
    """Return a signed HS256 JWT for the operator."""
    issued = now if now is not None else datetime.now(UTC)
    if issued.tzinfo is None:
        issued = issued.replace(tzinfo=UTC)
    payload: dict[str, Any] = {
        "sub": operator_id,
        "role": role,
        "iat": int(issued.timestamp()),
        "exp": int((issued + _TTL).timestamp()),
        "jti": str(uuid4()),
    }
    return jwt.encode(payload, secret, algorithm=_ALGORITHM)


def verify_token(token: str, secret: bytes, *, now: datetime | None = None) -> SessionClaims | None:
    """Return claims when the token is valid; otherwise ``None``."""
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=[_ALGORITHM],
            options=cast(Any, _DECODE_OPTIONS),
            leeway=0,
        )
    except jwt.PyJWTError:
        return None
    if now is not None:
        exp = payload.get("exp")
        if not isinstance(exp, int):
            return None
        instant = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
        if exp <= int(instant.timestamp()):
            return None
    sub = payload.get("sub")
    role = payload.get("role")
    exp_raw = payload.get("exp")
    jti = payload.get("jti")
    if not isinstance(sub, str) or not sub:
        return None
    if not isinstance(role, str) or role not in {"viewer", "analyst", "admin"}:
        return None
    if not isinstance(exp_raw, int):
        return None
    if not isinstance(jti, str) or not jti:
        return None
    return SessionClaims(
        operator_id=sub,
        role=role,
        exp=datetime.fromtimestamp(exp_raw, UTC),
        jti=jti,
    )


def seconds_until_expiry(claims: SessionClaims, *, now: datetime | None = None) -> float:
    """Seconds remaining before ``claims.exp``."""
    instant = now if now is not None else datetime.now(UTC)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    return (claims.exp - instant).total_seconds()


def needs_renewal(claims: SessionClaims, *, now: datetime | None = None) -> bool:
    """True when the session should be re-issued (under 15 minutes left)."""
    return seconds_until_expiry(claims, now=now) < _RENEW_WITHIN.total_seconds()
