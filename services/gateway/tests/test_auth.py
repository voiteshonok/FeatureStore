"""JWT session auth, RBAC, CSRF, and login helpers (ADR-020)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import jwt
import pytest
from harness import TEST_JWT_SECRET, mount, ok_json, operator_headers, send

from gateway.auth.jwt import issue_token, verify_token
from gateway.auth.password import passwords_match


def test_issue_and_verify_token() -> None:
    token = issue_token(operator_id="3", role="analyst", secret=TEST_JWT_SECRET)
    claims = verify_token(token, TEST_JWT_SECRET)
    assert claims is not None
    assert claims.operator_id == "3"
    assert claims.role == "analyst"
    assert claims.jti


def test_expired_token_is_rejected() -> None:
    past = datetime.now(UTC) - timedelta(hours=2)
    token = issue_token(
        operator_id="1",
        role="viewer",
        secret=TEST_JWT_SECRET,
        now=past,
    )
    assert verify_token(token, TEST_JWT_SECRET) is None


def test_wrong_key_is_rejected() -> None:
    token = issue_token(operator_id="1", role="admin", secret=TEST_JWT_SECRET)
    assert verify_token(token, b"\x00" * 32) is None


def test_alg_none_is_rejected() -> None:
    payload = {
        "sub": "1",
        "role": "admin",
        "iat": int(datetime.now(UTC).timestamp()),
        "exp": int((datetime.now(UTC) + timedelta(hours=1)).timestamp()),
        "jti": "x",
    }
    token = jwt.encode(payload, key="", algorithm="none")
    assert verify_token(token, TEST_JWT_SECRET) is None


def test_passwords_match_runs_dummy_check_for_missing_hash(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bytes] = []
    real = __import__("bcrypt").checkpw

    def tracked(password: bytes, hashed: bytes) -> bool:
        calls.append(hashed)
        return real(password, hashed)

    monkeypatch.setattr("gateway.auth.password.bcrypt.checkpw", tracked)
    assert passwords_match("secret", None) is False
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("method", "path", "api_key", "role", "csrf", "status"),
    [
        ("POST", "/v1/features", True, None, False, 200),
        ("GET", "/v1/alerts", True, None, False, 403),
        ("GET", "/v1/alerts", False, "viewer", False, 200),
        ("GET", "/v1/rules", False, "viewer", False, 200),
        ("PATCH", "/v1/alerts/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", False, "viewer", True, 403),
        ("PATCH", "/v1/alerts/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", False, "analyst", True, 200),
        ("PATCH", "/v1/alerts/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", False, "admin", True, 200),
        ("POST", "/v1/features", False, "viewer", True, 200),
    ],
)
async def test_rbac_matrix(
    method: str,
    path: str,
    api_key: bool,
    role: str | None,
    csrf: bool,
    status: int,
) -> None:
    gateway = await mount(ok_json())
    try:
        headers = operator_headers(role=role or "viewer", csrf=csrf) if not api_key else None
        response = await send(
            gateway.app,
            method,
            path,
            api_key="dev-scoring-key" if api_key else None,
            content=b"{}" if method != "GET" else None,
            headers=headers,
        )
    finally:
        await gateway.aclose()
    assert response.status_code == status


async def test_csrf_required_for_cookie_patch() -> None:
    gateway = await mount(ok_json())
    try:
        response = await send(
            gateway.app,
            "PATCH",
            "/v1/alerts/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            api_key=None,
            content=b"{}",
            headers=operator_headers(csrf=False),
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 403
    assert response.json()["type"] == "forbidden"
    assert response.json()["title"] == "csrf header required"


async def test_invalid_cookie_is_401() -> None:
    gateway = await mount(ok_json())
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/alerts",
            api_key=None,
            headers={"Cookie": "fs_session=not-a-jwt"},
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 401
    assert response.json()["title"] == "invalid session"


async def test_logout_clears_cookie() -> None:
    gateway = await mount(ok_json())
    try:
        response = await send(
            gateway.app,
            "POST",
            "/v1/auth/logout",
            api_key=None,
            headers=operator_headers(csrf=True),
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 204
    assert "fs_session=" in response.headers.get("set-cookie", "")
    assert "Max-Age=0" in response.headers.get("set-cookie", "") or (
        "max-age=0" in response.headers.get("set-cookie", "").lower()
    )


async def test_me_returns_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    from gateway.auth.operators import OperatorRow

    async def fake_fetch(_pool: object, operator_id: str) -> OperatorRow:
        assert operator_id == "9"
        return OperatorRow(operator_id=9, email="analyst@featurestore.local", role="analyst")

    monkeypatch.setattr("gateway.api.fetch_operator_by_id", fake_fetch)
    gateway = await mount(ok_json(), pool=MagicMock())
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/auth/me",
            api_key=None,
            headers=operator_headers(operator_id="9", role="analyst"),
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert response.json() == {
        "operator_id": "9",
        "email": "analyst@featurestore.local",
        "role": "analyst",
    }


async def test_login_success_sets_cookie(monkeypatch: pytest.MonkeyPatch) -> None:
    import bcrypt

    from gateway.auth.operators import OperatorRow

    hashed = bcrypt.hashpw(b"demo-analyst-pass", bcrypt.gensalt(rounds=4)).decode()

    async def fake_fetch(_pool: object, email: str) -> OperatorRow:
        assert email == "analyst@featurestore.local"
        return OperatorRow(
            operator_id=2,
            email=email,
            role="analyst",
            password_hash=hashed,
        )

    monkeypatch.setattr("gateway.api.fetch_operator_by_email", fake_fetch)
    gateway = await mount(ok_json(), pool=MagicMock())
    try:
        response = await send(
            gateway.app,
            "POST",
            "/v1/auth/login",
            api_key=None,
            json={"email": "analyst@featurestore.local", "password": "demo-analyst-pass"},
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 204
    cookie = response.headers.get("set-cookie", "")
    assert "fs_session=" in cookie
    assert "HttpOnly" in cookie
    assert "Path=/api" in cookie
    assert "SameSite=strict" in cookie or "SameSite=Strict" in cookie


async def test_login_failure_same_message(monkeypatch: pytest.MonkeyPatch) -> None:
    import bcrypt

    from gateway.auth.operators import OperatorRow

    hashed = bcrypt.hashpw(b"right", bcrypt.gensalt(rounds=4)).decode()

    async def fake_fetch(_pool: object, email: str) -> OperatorRow | None:
        if email == "analyst@featurestore.local":
            return OperatorRow(
                operator_id=2,
                email=email,
                role="analyst",
                password_hash=hashed,
            )
        return None

    monkeypatch.setattr("gateway.api.fetch_operator_by_email", fake_fetch)
    gateway = await mount(ok_json(), pool=MagicMock())
    try:
        bad_password = await send(
            gateway.app,
            "POST",
            "/v1/auth/login",
            api_key=None,
            json={"email": "analyst@featurestore.local", "password": "wrong"},
        )
        missing = await send(
            gateway.app,
            "POST",
            "/v1/auth/login",
            api_key=None,
            json={"email": "nobody@featurestore.local", "password": "wrong"},
        )
    finally:
        await gateway.aclose()
    assert bad_password.status_code == 401
    assert missing.status_code == 401
    assert bad_password.json()["title"] == missing.json()["title"] == "invalid credentials"


async def test_session_renewal_sets_cookie(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gateway.api.should_renew", lambda _claims: True)
    gateway = await mount(ok_json())
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/alerts",
            api_key=None,
            headers=operator_headers(),
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert "fs_session=" in response.headers.get("set-cookie", "")


def test_settings_from_env_reads_cookie_secure(monkeypatch: pytest.MonkeyPatch) -> None:
    from gateway.settings import Settings

    monkeypatch.setenv("GATEWAY_COOKIE_SECURE", "true")
    monkeypatch.delenv("GATEWAY_API_KEYS", raising=False)
    settings = Settings.from_env()
    assert settings.cookie_secure is True
