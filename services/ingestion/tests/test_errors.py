"""Global error handlers: Problem Details, request_id, PAN masking."""

from __future__ import annotations

import logging

import pytest
from httpx import ASGITransport, AsyncClient

from ingestion.app import Settings, create_app

_PAN = "4111111111111111"


async def test_unhandled_exception_is_problem_details_without_pan(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = create_app(Settings(), include_test_routes=True)
    with caplog.at_level(logging.ERROR, logger="ingestion"):
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/__test__/boom",
                headers={"X-Request-ID": "boom-req-1"},
            )
    assert response.status_code == 500
    body = response.json()
    assert body == {
        "type": "internal_error",
        "title": "Internal server error",
        "status": 500,
        "request_id": "boom-req-1",
        "errors": [],
    }
    assert response.headers["x-request-id"] == "boom-req-1"
    assert _PAN not in response.text
    assert _PAN not in caplog.text


async def test_validation_error_lists_field_name() -> None:
    app = create_app(Settings(), include_test_routes=True)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post("/v1/events", json={"not": "a payment"})
    assert response.status_code == 422
    body = response.json()
    assert body["type"] == "validation_error"
    assert body["status"] == 422
    assert "request_id" in body
    assert body["errors"]
    assert all("field" in err and "message" in err for err in body["errors"])
