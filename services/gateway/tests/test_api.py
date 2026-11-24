"""API keys, rate limits, correlation, and the static health check."""

import json
import logging
import sys
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from fastapi.routing import APIRoute
from harness import make_settings, mount, ok_json, operator_headers, send
from prometheus_client import CollectorRegistry

from gateway.api import app, export_openapi
from gateway.auth.access import AuthContext, rbac_allowed
from gateway.logging import JsonFormatter
from gateway.metrics import gateway_metrics
from gateway.settings import Settings, parse_api_keys


def test_parse_api_keys_splits_on_the_first_colon() -> None:
    assert parse_api_keys(" scoring:k1 , dashboard:ab:cd ") == {
        "k1": "scoring",
        "ab:cd": "dashboard",
    }
    assert parse_api_keys(" , ") == {}


@pytest.mark.parametrize(
    "raw",
    ["scoring", "scoring:", ":secret", "scoring:k1,scoring:k2", "scoring:k1,dashboard:k1"],
)
def test_parse_api_keys_rejects_bad_entries(raw: str) -> None:
    with pytest.raises(ValueError, match="client:key|unique"):
        parse_api_keys(raw)


def test_settings_reject_an_unparsable_rate() -> None:
    with pytest.raises(ValueError):
        Settings(api_keys={}, rate_limit="often")


def test_settings_from_env_uses_the_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GATEWAY_API_KEYS", raising=False)
    monkeypatch.delenv("GATEWAY_RATE_LIMIT", raising=False)
    monkeypatch.delenv("FEATURES_URL", raising=False)
    monkeypatch.delenv("DETECTION_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    settings = Settings.from_env()
    assert settings.rate_limit == "100/second"
    assert settings.api_keys == {}
    assert settings.features_url == "http://localhost:8001"
    assert settings.detection_url == "http://localhost:8002"


def test_json_formatter_includes_request_fields() -> None:
    record = logging.LogRecord(
        name="gateway",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request",
        args=(),
        exc_info=None,
    )
    record.request_id = "trace-1"
    record.client = "scoring"
    record.method = "POST"
    record.path = "/v1/features"
    record.status = 200
    record.latency_ms = 3
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "request"
    assert payload["request_id"] == "trace-1"
    assert payload["client"] == "scoring"
    assert payload["status"] == 200
    assert payload["latency_ms"] == 3
    assert "exception" not in payload

    try:
        raise OSError("down")
    except OSError:
        record.exc_info = sys.exc_info()
    failed = json.loads(JsonFormatter().format(record))
    assert "OSError" in str(failed["exception"])


async def test_missing_key_is_401_and_unknown_key_is_403() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ok": True})

    gateway = await mount(handler)
    try:
        missing = await send(gateway.app, "POST", "/v1/features", api_key=None, content=b"{}")
        unknown = await send(gateway.app, "POST", "/v1/features", api_key="nope", content=b"{}")
        allowed = await send(gateway.app, "POST", "/v1/features", content=b"{}")
    finally:
        await gateway.aclose()

    assert missing.status_code == 401
    assert missing.json()["type"] == "unauthorized"
    assert missing.json()["title"] == "missing credentials"
    assert "request_id" in missing.json()
    assert unknown.status_code == 403
    assert unknown.json()["type"] == "forbidden"
    assert unknown.json()["title"] == "unknown api key"
    assert allowed.status_code == 200
    assert len(calls) == 1
    assert "x-api-key" not in {key.lower() for key in calls[0].headers}


async def test_health_does_not_check_the_key_or_upstreams() -> None:
    gateway = await mount(ok_json())
    try:
        response = await send(gateway.app, "GET", "/health", api_key=None)
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_alert_route_label_is_the_template() -> None:
    registry = CollectorRegistry()
    alert_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    gateway = await mount(ok_json(), metrics=gateway_metrics(registry))
    try:
        response = await send(
            gateway.app,
            "PATCH",
            f"/v1/alerts/{alert_id}",
            api_key=None,
            content=b"{}",
            headers=operator_headers(csrf=True),
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert (
        registry.get_sample_value(
            "gateway_request_duration_seconds_count",
            {"route": "/v1/alerts/{alert_id}", "method": "PATCH", "status": "200"},
        )
        == 1
    )
    for family in registry.collect():
        for sample in family.samples:
            for value in sample.labels.values():
                assert alert_id not in value


async def test_rate_limit_increments_the_client_counter() -> None:
    registry = CollectorRegistry()
    gateway = await mount(
        ok_json(),
        make_settings(rate_limit="1/minute"),
        metrics=gateway_metrics(registry),
    )
    try:
        first = await send(gateway.app, "POST", "/v1/features", content=b"{}")
        second = await send(gateway.app, "POST", "/v1/features", content=b"{}")
    finally:
        await gateway.aclose()
    assert first.status_code == 200
    assert second.status_code == 429
    assert registry.get_sample_value("gateway_rate_limited_total", {"client": "scoring"}) == 1


async def test_rejected_auth_does_not_consume_the_rate_limit() -> None:
    gateway = await mount(ok_json(), make_settings(rate_limit="1/minute"))
    try:
        missing = await send(gateway.app, "POST", "/v1/features", api_key=None)
        first = await send(gateway.app, "POST", "/v1/features")
        second = await send(gateway.app, "POST", "/v1/features")
    finally:
        await gateway.aclose()
    assert missing.status_code == 401
    assert first.status_code == 200
    assert second.status_code == 429


async def test_rate_limit_is_shared_across_routes_and_per_client() -> None:
    from harness import OTHER_KEY

    gateway = await mount(ok_json(), make_settings(rate_limit="2/minute"))
    try:
        first = await send(gateway.app, "POST", "/v1/features")
        second = await send(gateway.app, "POST", "/v1/features")
        blocked = await send(gateway.app, "POST", "/v1/features")
        other = await send(gateway.app, "POST", "/v1/features", api_key=OTHER_KEY)
    finally:
        await gateway.aclose()

    assert first.status_code == 200
    assert second.status_code == 200
    assert blocked.status_code == 429
    assert blocked.json()["type"] == "rate_limited"
    assert blocked.json()["title"] == "rate limit exceeded"
    assert blocked.json()["status"] == 429
    assert "request_id" in blocked.json()
    retry_after = blocked.headers["retry-after"]
    assert int(retry_after) >= 0
    assert other.status_code == 200
    UUID(blocked.headers["x-request-id"])


async def test_request_id_is_generated_and_forwarded() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["x-request-id"])
        return httpx.Response(200, json={"ok": True})

    gateway = await mount(handler)
    try:
        generated = await send(gateway.app, "POST", "/v1/features", content=b"{}")
        preserved = await send(
            gateway.app,
            "POST",
            "/v1/features",
            content=b"{}",
            headers={"X-Request-ID": "trace-1"},
        )
        blank = await send(
            gateway.app,
            "POST",
            "/v1/features",
            content=b"{}",
            headers={"X-Request-ID": "   "},
        )
    finally:
        await gateway.aclose()

    assert generated.headers["x-request-id"] == seen[0]
    UUID(generated.headers["x-request-id"])
    assert preserved.headers["x-request-id"] == "trace-1"
    assert seen[1] == "trace-1"
    assert blank.headers["x-request-id"] == seen[2]
    UUID(blank.headers["x-request-id"])
    assert blank.headers["x-request-id"].strip() != ""


async def test_request_log_carries_the_client_and_request_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="gateway")
    gateway = await mount(ok_json())
    try:
        response = await send(
            gateway.app,
            "POST",
            "/v1/features",
            content=b"{}",
            headers={"X-Request-ID": "trace-9"},
        )
    finally:
        await gateway.aclose()
    matched = [
        record
        for record in caplog.records
        if record.getMessage() == "request" and getattr(record, "request_id", None) == "trace-9"
    ]
    assert matched
    record = matched[-1]
    assert getattr(record, "client", None) == "scoring"
    assert getattr(record, "method", None) == "POST"
    assert getattr(record, "path", None) == "/v1/features"
    assert getattr(record, "status", None) == 200
    assert response.status_code == 200


def test_openapi_documents_the_business_routes(tmp_path: Path) -> None:
    destination = tmp_path / "gateway.json"
    export_openapi(str(destination))
    document = json.loads(destination.read_text(encoding="utf-8"))
    assert "/v1/events" not in document["paths"]
    post = document["paths"]["/v1/features"]["post"]
    assert post["security"] == [{"ApiKey": []}, {"SessionCookie": []}]
    assert document["components"]["securitySchemes"]["ApiKey"]["name"] == "X-API-Key"
    assert document["components"]["securitySchemes"]["SessionCookie"]["name"] == "fs_session"
    assert "FeatureRequest" in document["components"]["schemas"]
    assert "AlertOut" in document["components"]["schemas"]
    assert "CatalogEntry" in document["components"]["schemas"]
    assert "MeOut" in document["components"]["schemas"]
    assert "/v1/alerts" in document["paths"]
    assert "/v1/alerts/{alert_id}" in document["paths"]
    assert "get" in document["paths"]["/v1/alerts/{alert_id}"]
    assert "/v1/rules" in document["paths"]
    assert "AlertListOut" in document["components"]["schemas"]
    assert "ProblemDetails" in document["components"]["schemas"]
    assert "/v1/catalog" in document["paths"]
    assert "/v1/auth/login" in document["paths"]
    assert "/v1/auth/logout" in document["paths"]
    assert "/v1/auth/me" in document["paths"]
    assert "/health" in document["paths"]
    assert document["paths"]["/health"]["get"]["security"] == []
    assert document["paths"]["/ready"]["get"]["security"] == []
    assert document["info"]["version"] == "1.0.0"
    assert document["servers"] == [{"url": "http://localhost:8080"}]
    alerts = document["paths"]["/v1/alerts"]["get"]
    assert alerts["security"] == [{"SessionCookie": []}]
    patch = document["paths"]["/v1/alerts/{alert_id}"]["patch"]
    assert patch["security"] == [{"SessionCookie": []}]
    assert "analyst" in patch["description"]
    assert "X-Requested-With" in patch["description"]


_SKIP_PREFIXES = ("/docs", "/redoc", "/openapi.json")


def _expected_security(method: str, path: str) -> list[dict[str, list[str]]]:
    api_key = AuthContext(kind="api_key", client="scoring")
    session_ok = any(
        rbac_allowed(
            method,
            path,
            AuthContext(kind="session", client="operator:1", operator_id="1", role=role),
        )
        for role in ("viewer", "analyst", "admin")
    )
    schemes: list[dict[str, list[str]]] = []
    if rbac_allowed(method, path, api_key):
        schemes.append({"ApiKey": []})
    if session_ok:
        schemes.append({"SessionCookie": []})
    return schemes


def test_openapi_security_matches_rbac() -> None:
    document = app.openapi()
    seen = False
    for route in app.routes:
        if not isinstance(route, APIRoute) or not route.include_in_schema:
            continue
        if route.path.startswith(_SKIP_PREFIXES):
            continue
        path_item = document["paths"][route.path]
        for method in route.methods:
            if method in {"HEAD", "OPTIONS"}:
                continue
            operation = path_item[method.lower()]
            assert "security" in operation
            assert operation["security"] == _expected_security(method, route.path)
            assert operation.get("summary")
            assert operation.get("description")
            seen = True
    assert seen
