"""Global error handlers for features-api."""

from __future__ import annotations

import logging

import pytest
from httpx import ASGITransport, AsyncClient

from features.api import create_app
from features.settings import Settings

_PAN = "4111111111111111"


async def test_unhandled_exception_is_problem_details_without_pan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = create_app(Settings(), include_test_routes=True)
    with caplog.at_level(logging.ERROR, logger="features"):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/__test__/boom",
                headers={"X-Request-ID": "feat-boom-1"},
            )
    assert response.status_code == 500
    body = response.json()
    assert body["type"] == "internal_error"
    assert body["title"] == "Internal server error"
    assert body["request_id"] == "feat-boom-1"
    assert response.headers["x-request-id"] == "feat-boom-1"
    assert _PAN not in response.text
    assert _PAN not in caplog.text


async def test_validation_error_lists_field_name() -> None:
    app = create_app(Settings(), include_test_routes=True)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/v1/features", json={})
    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "validation_error"
    assert body["errors"]
    assert all("field" in err for err in body["errors"])
