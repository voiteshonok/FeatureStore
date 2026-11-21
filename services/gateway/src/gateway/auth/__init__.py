"""Operator session auth (ADR-020)."""

from gateway.auth.cookies import (
    COOKIE_NAME,
    clear_session_cookie,
    read_session_token,
    set_session_cookie,
    should_renew,
)
from gateway.auth.jwt import SessionClaims, issue_token, verify_token
from gateway.auth.operators import OperatorRow, fetch_operator_by_email, fetch_operator_by_id
from gateway.auth.password import passwords_match
from gateway.auth.secret import JwtSecretError, load_jwt_secret

__all__ = [
    "COOKIE_NAME",
    "JwtSecretError",
    "OperatorRow",
    "SessionClaims",
    "clear_session_cookie",
    "fetch_operator_by_email",
    "fetch_operator_by_id",
    "issue_token",
    "load_jwt_secret",
    "passwords_match",
    "read_session_token",
    "set_session_cookie",
    "should_renew",
    "verify_token",
]
