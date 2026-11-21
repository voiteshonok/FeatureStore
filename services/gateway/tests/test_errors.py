"""Global error handlers for gateway-owned responses."""

from __future__ import annotations

import logging

import pytest
from harness import make_settings, send
from httpx import ASGITransport, AsyncClient

from gateway.api import create_app

_PAN = "4111111111111111"


async def test_unhandled_exception_is_problem_details_without_pan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = create_app(make_settings(), include_test_routes=True)
    with caplog.at_level(logging.ERROR, logger="gateway"):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/__test__/boom",
                headers={"X-Request-ID": "gw-boom-1"},
            )
    assert response.status_code == 500
    body = response.json()
    assert body["type"] == "internal_error"
    assert body["title"] == "Internal server error"
    assert body["request_id"] == "gw-boom-1"
    assert response.headers["x-request-id"] == "gw-boom-1"
    assert _PAN not in response.text
    assert _PAN not in caplog.text


async def test_missing_api_key_is_problem_details() -> None:
    app = create_app(make_settings(), include_test_routes=True)
    response = await send(app, "GET", "/v1/alerts", api_key=None)
    assert response.status_code == 401
    assert response.json()["type"] == "unauthorized"
    assert "request_id" in response.json()
