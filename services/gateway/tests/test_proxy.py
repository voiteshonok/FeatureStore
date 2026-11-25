"""Retries, timeouts, and upstream status codes."""

import httpx
import pytest
from harness import mount, operator_headers, send

from gateway.proxy import pause


def _failing(error: type[httpx.TransportError]) -> tuple[list[int], object]:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise error("down", request=request)

    return calls, handler  # type: ignore[return-value]


@pytest.mark.parametrize(
    ("method", "path", "detail", "attempts", "sleeps", "session"),
    [
        ("GET", "/v1/alerts?status=open", "detection unavailable", 3, [0.1, 0.4], True),
        ("POST", "/v1/features", "features unavailable", 2, [0.05], False),
        ("PATCH", "/v1/alerts/not-a-uuid", "detection unavailable", 1, [], True),
    ],
)
async def test_connect_error_retry_policy(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    detail: str,
    attempts: int,
    sleeps: list[float],
    session: bool,
) -> None:
    recorded: list[float] = []

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    calls, handler = _failing(httpx.ConnectError)
    gateway = await mount(handler)  # type: ignore[arg-type]
    try:
        headers = operator_headers(csrf=method == "PATCH") if session else None
        response = await send(
            gateway.app,
            method,
            path,
            api_key=None if session else "dev-scoring-key",
            content=b"{}",
            headers=headers,
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == attempts
    assert recorded == sleeps
    assert response.status_code == 503
    assert response.json()["type"] == "upstream_unavailable"
    assert response.json()["title"] == detail
    assert response.json()["status"] == 503
    assert "request_id" in response.json()


async def test_get_succeeds_on_the_second_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[float] = []
    calls = {"n": 0}

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("slow")
        return httpx.Response(200, json={"alerts": []})

    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/alerts?status=open",
            api_key=None,
            headers=operator_headers(),
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == 2
    assert recorded == [0.1]
    assert response.status_code == 200
    assert response.json() == {"alerts": []}


async def test_post_features_succeeds_after_connect_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[float] = []
    calls = {"n": 0}

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("down", request=request)
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    gateway = await mount(handler)
    try:
        response = await send(gateway.app, "POST", "/v1/features", content=b"{}")
    finally:
        await gateway.aclose()
    assert calls["n"] == 2
    assert recorded == [0.05]
    assert response.status_code == 200
    assert response.json() == {"ok": True}


async def test_post_features_read_timeout_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded: list[float] = []

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    calls, handler = _failing(httpx.ReadTimeout)
    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    gateway = await mount(handler)  # type: ignore[arg-type]
    try:
        response = await send(gateway.app, "POST", "/v1/features", content=b"{}")
    finally:
        await gateway.aclose()
    assert calls["n"] == 1
    assert recorded == []
    assert response.status_code == 504
    assert response.json()["type"] == "upstream_timeout"
    assert response.json()["title"] == "features timeout"


async def test_patch_read_timeout_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[float] = []

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    calls, handler = _failing(httpx.ReadTimeout)
    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    gateway = await mount(handler)  # type: ignore[arg-type]
    try:
        response = await send(
            gateway.app,
            "PATCH",
            "/v1/alerts/not-a-uuid",
            api_key=None,
            content=b"{}",
            headers=operator_headers(csrf=True),
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == 1
    assert recorded == []
    assert response.status_code == 504
    assert response.json()["type"] == "upstream_timeout"
    assert response.json()["title"] == "detection timeout"


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (409, b'{"detail":"invalid transition"}'),
        (404, b'{"detail":"alert not found"}'),
    ],
)
async def test_upstream_4xx_passes_through(status: int, body: bytes) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        assert request.url.path == "/v1/alerts/not-a-uuid"
        return httpx.Response(status, content=body, headers={"content-type": "application/json"})

    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "PATCH",
            "/v1/alerts/not-a-uuid",
            api_key=None,
            content=body,
            headers={**operator_headers(csrf=True), "content-type": "application/json"},
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == 1
    assert response.status_code == status
    assert response.content == body


async def test_upstream_5xx_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[float] = []
    calls = {"n": 0}

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503, json={"detail": "redis unavailable"})

    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/alerts?status=open",
            api_key=None,
            headers=operator_headers(),
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == 1
    assert recorded == []
    assert response.status_code == 503
    assert response.json() == {"detail": "redis unavailable"}


async def test_post_forwards_the_raw_body_and_query() -> None:
    seen: list[tuple[bytes, str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.content, str(request.url), request.headers.get("content-type")))
        return httpx.Response(200, json={"ok": True})

    body = b'{"card_token":"card-1"}'
    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "POST",
            "/v1/features",
            content=body,
            headers={"content-type": "application/json"},
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert seen == [(body, "http://features.test/v1/features", "application/json")]


async def test_get_forwards_the_query_string() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json=[])

    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/alerts?status=open&limit=10",
            api_key=None,
            headers=operator_headers(),
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert seen == ["http://detection.test/v1/alerts?status=open&limit=10"]


async def test_timeout_maps_to_504(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: list[float] = []

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    calls, handler = _failing(httpx.ReadTimeout)
    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    gateway = await mount(handler)  # type: ignore[arg-type]
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/alerts?status=open",
            api_key=None,
            headers=operator_headers(),
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == 3
    assert recorded == [0.1, 0.4]
    assert response.status_code == 504
    assert response.json()["type"] == "upstream_timeout"
    assert response.json()["title"] == "detection timeout"
    assert response.json()["status"] == 504


async def test_pause_sleeps(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("gateway.proxy.asyncio.sleep", fake_sleep)
    await pause(0.1)
    assert slept == [0.1]


async def test_forged_operator_headers_are_stripped() -> None:
    seen: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers)
        return httpx.Response(200, json={"ok": True})

    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "PATCH",
            "/v1/alerts/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            api_key=None,
            content=b'{"status":"acknowledged"}',
            headers={
                **operator_headers(operator_id="7", role="analyst", csrf=True),
                "X-Operator-Id": "forged",
                "X-Operator-Role": "admin",
                "content-type": "application/json",
            },
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert seen[0]["x-operator-id"] == "7"
    assert seen[0]["x-operator-role"] == "analyst"


async def test_api_key_does_not_forward_operator_headers() -> None:
    seen: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers)
        return httpx.Response(200, json={"ok": True})

    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "POST",
            "/v1/features",
            content=b"{}",
            headers={
                "X-Operator-Id": "forged",
                "X-Operator-Role": "admin",
                "content-type": "application/json",
            },
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert "x-operator-id" not in seen[0]
    assert "x-operator-role" not in seen[0]
