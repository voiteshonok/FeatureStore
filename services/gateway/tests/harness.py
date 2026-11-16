"""App factory helpers for gateway unit tests.

Named ``harness`` so it does not shadow ``support`` in the other services.
The tests directory is on ``sys.path`` for the whole pytest session.
"""

from collections.abc import Awaitable, Callable

import httpx
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from gateway.api import create_app
from gateway.metrics import GatewayMetrics
from gateway.settings import Settings

KEY = "dev-scoring-key"
DASHBOARD_KEY = "dev-dashboard-key"
Handler = Callable[[httpx.Request], httpx.Response | Awaitable[httpx.Response]]


def make_settings(
    *,
    api_keys: dict[str, str] | None = None,
    rate_limit: str = "100/second",
) -> Settings:
    return Settings(
        api_keys={KEY: "scoring", DASHBOARD_KEY: "dashboard"} if api_keys is None else api_keys,
        rate_limit=rate_limit,
        features_url="http://features.test",
        detection_url="http://detection.test",
        database_url="postgresql://svc_gateway:featurestore@localhost/featurestore",
        metrics_port=0,
    )


class Gateway:
    """A gateway whose upstream client is a mock transport."""

    def __init__(self, app: FastAPI, client: httpx.AsyncClient) -> None:
        self.app = app
        self.client = client

    async def aclose(self) -> None:
        await self.client.aclose()


async def mount(
    handler: Handler,
    settings: Settings | None = None,
    pool: object | None = None,
    metrics: GatewayMetrics | None = None,
) -> Gateway:
    app = create_app(settings or make_settings(), metrics=metrics)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app.state.client = client
    if pool is not None:
        app.state.pool = pool
    return Gateway(app, client)


async def send(
    app: FastAPI,
    method: str,
    url: str,
    *,
    api_key: str | None = KEY,
    headers: dict[str, str] | None = None,
    content: bytes | None = None,
    json: object | None = None,
) -> httpx.Response:
    sent = dict(headers or {})
    if api_key is not None:
        sent["X-API-Key"] = api_key
    kwargs: dict[str, object] = {}
    if content is not None:
        kwargs["content"] = content
    if json is not None:
        kwargs["json"] = json
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.request(method, url, headers=sent, **kwargs)


def ok_json(body: object | None = None) -> Handler:
    payload = {"ok": True} if body is None else body

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    return handler
