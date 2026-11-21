"""Retries, timeouts, and upstream status codes."""

import httpx
import pytest
from harness import mount, send

from gateway.proxy import pause


def _failing(error: type[httpx.TransportError]) -> tuple[list[int], object]:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise error("down", request=request)

    return calls, handler  # type: ignore[return-value]


@pytest.mark.parametrize(
    ("method", "path", "detail", "attempts", "sleeps"),
    [
        ("GET", "/v1/alerts?status=open", "detection unavailable", 3, [0.1, 0.4]),
        ("POST", "/v1/features", "features unavailable", 1, []),
        ("PATCH", "/v1/alerts/not-a-uuid", "detection unavailable", 1, []),
    ],
)
async def test_transport_failures_retry_only_get(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    detail: str,
    attempts: int,
    sleeps: list[float],
) -> None:
    recorded: list[float] = []

    async def fake_pause(seconds: float) -> None:
        recorded.append(seconds)

    monkeypatch.setattr("gateway.proxy.pause", fake_pause)
    calls, handler = _failing(httpx.ConnectError)
    gateway = await mount(handler)  # type: ignore[arg-type]
    try:
        response = await send(gateway.app, method, path, content=b"{}")
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
        response = await send(gateway.app, "GET", "/v1/alerts?status=open")
    finally:
        await gateway.aclose()
    assert calls["n"] == 2
    assert recorded == [0.1]
    assert response.status_code == 200
    assert response.json() == {"alerts": []}


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
            content=body,
            headers={"content-type": "application/json"},
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
        response = await send(gateway.app, "GET", "/v1/alerts?status=open")
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
        response = await send(gateway.app, "GET", "/v1/alerts?status=open&limit=10")
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
        response = await send(gateway.app, "GET", "/v1/alerts?status=open")
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
