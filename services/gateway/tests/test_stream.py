"""SSE stream proxy and RBAC (ADR-021)."""

from __future__ import annotations

import httpx
import pytest
from harness import mount, operator_headers, send


async def test_scoring_key_cannot_open_stream() -> None:
    gateway = await mount(lambda _r: httpx.Response(200, content=b"retry: 3000\n\n"))
    try:
        response = await send(gateway.app, "GET", "/v1/stream/alerts")
    finally:
        await gateway.aclose()
    assert response.status_code == 403


async def test_viewer_session_can_open_stream() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            content=b"retry: 3000\n\nevent: stats\ndata: {}\n\n",
            headers={"content-type": "text/event-stream"},
        )

    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/stream/alerts",
            api_key=None,
            headers={**operator_headers(role="viewer"), "Last-Event-ID": "42"},
        )
    finally:
        await gateway.aclose()
    assert response.status_code == 200
    assert "retry: 3000" in response.text
    assert seen[0].headers.get("last-event-id") == "42"
    assert "x-operator-id" in {k.lower() for k in seen[0].headers.keys()}


async def test_stream_proxy_does_not_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("down", request=request)

    gateway = await mount(handler)
    try:
        response = await send(
            gateway.app,
            "GET",
            "/v1/stream/alerts",
            api_key=None,
            headers=operator_headers(),
        )
    finally:
        await gateway.aclose()
    assert calls["n"] == 1
    assert response.status_code == 503
