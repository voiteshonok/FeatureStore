"""Readiness probes."""

import asyncio
import time

import httpx
import pytest
from harness import mount, send


def _handler(features: int, detection: int) -> object:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "features.test":
            return httpx.Response(features, json={"status": "ok"})
        if request.url.host == "detection.test":
            return httpx.Response(detection, json={"status": "ok"})
        return httpx.Response(404)

    return handler


async def test_ready_reports_each_upstream() -> None:
    healthy = await mount(_handler(200, 200))  # type: ignore[arg-type]
    try:
        ready = await send(healthy.app, "GET", "/ready", api_key=None)
    finally:
        await healthy.aclose()
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready", "features": "ok", "detection": "ok"}

    degraded = await mount(_handler(200, 500))  # type: ignore[arg-type]
    try:
        down = await send(degraded.app, "GET", "/ready", api_key=None)
    finally:
        await degraded.aclose()
    assert down.status_code == 503
    assert down.json() == {"status": "not_ready", "features": "ok", "detection": "down"}


async def test_ready_stays_inside_the_budget_when_a_probe_hangs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("gateway.api._READY_BUDGET_S", 0.1)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "features.test":
            await asyncio.sleep(2)
        return httpx.Response(200, json={"status": "ok"})

    gateway = await mount(handler)
    try:
        started = time.perf_counter()
        response = await send(gateway.app, "GET", "/ready", api_key=None)
        elapsed = time.perf_counter() - started
    finally:
        await gateway.aclose()
    assert elapsed < 1
    assert response.status_code == 503
    assert response.json()["features"] == "down"
    assert response.json()["detection"] == "ok"
