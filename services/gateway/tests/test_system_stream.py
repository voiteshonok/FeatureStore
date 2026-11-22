"""System SSE route auth and collector fan-out (ADR-022)."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
from harness import make_settings, mount, send

from gateway.system.collector import SystemCollector
from gateway.system.hub import SystemHub
from gateway.system.sse import sse_frames
from gateway.system.state import ProbeBundle, ReadyProbe, WorkerProbe, build_snapshot


def _snapshot() -> object:
    now = datetime.now(UTC)
    return build_snapshot(
        ProbeBundle(
            checked_at=now,
            gateway_status="up",
            ready={
                "ingestion": ReadyProbe(
                    service="ingestion",
                    status="up",
                    checked_at=now,
                    deps={"kafka": "up", "postgres": "up"},
                ),
                "features-api": ReadyProbe(
                    service="features-api",
                    status="up",
                    checked_at=now,
                    deps={"redis": "up", "postgres": "up"},
                ),
                "detection": ReadyProbe(
                    service="detection",
                    status="up",
                    checked_at=now,
                    deps={"kafka": "up", "postgres": "up"},
                ),
            },
            workers={
                "features-worker": WorkerProbe(
                    name="features-worker",
                    status="up",
                    checked_at=now,
                    lag=0,
                ),
                "features-offline": WorkerProbe(
                    name="features-offline",
                    status="up",
                    checked_at=now,
                    lag=0,
                ),
            },
            prom_ok=True,
        )
    )


async def test_scoring_key_cannot_open_system_stream() -> None:
    gateway = await mount(lambda _r: httpx.Response(200))
    try:
        response = await send(gateway.app, "GET", "/v1/stream/system")
    finally:
        await gateway.aclose()
    assert response.status_code == 403


async def test_anonymous_cannot_open_system_stream() -> None:
    gateway = await mount(lambda _r: httpx.Response(200))
    try:
        response = await send(gateway.app, "GET", "/v1/stream/system", api_key=None)
    finally:
        await gateway.aclose()
    assert response.status_code == 401


async def test_sse_frames_emits_snapshot_then_change() -> None:
    hub = SystemHub()
    await hub.publish("snapshot", _snapshot())
    chunks: list[bytes] = []
    agen = sse_frames(hub)
    try:
        async for chunk in agen:
            chunks.append(chunk)
            joined = b"".join(chunks)
            if b"event: snapshot" in joined:
                break
        assert b"event: snapshot" in b"".join(chunks)
        assert b'"mode":"normal"' in b"".join(chunks).replace(b" ", b"")

        degraded = build_snapshot(
            ProbeBundle(
                checked_at=datetime.now(UTC),
                gateway_status="up",
                ready={
                    "features-api": ReadyProbe(
                        service="features-api",
                        status="down",
                        checked_at=datetime.now(UTC),
                        deps={"redis": "down", "postgres": "up"},
                    ),
                },
                workers={},
                prom_ok=False,
            )
        )
        await hub.publish("change", degraded)
        async for chunk in agen:
            chunks.append(chunk)
            if b"event: change" in chunk:
                break
        assert b"event: change" in b"".join(chunks)
    finally:
        await agen.aclose()


async def test_collector_probe_count_independent_of_subscribers() -> None:
    settings = make_settings()
    hub = SystemHub()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ready"):
            host = request.url.host
            if host == "features.test":
                return httpx.Response(
                    200,
                    json={"status": "ready", "redis": "ok", "postgres": "ok"},
                )
            if host == "detection.test":
                return httpx.Response(
                    200,
                    json={"status": "ready", "kafka": "ok", "postgres": "ok"},
                )
            if host == "ingestion.test":
                return httpx.Response(
                    200,
                    json={"status": "ready", "kafka": "ok", "postgres": "ok"},
                )
        if "/api/v1/query" in str(request.url):
            return httpx.Response(
                200,
                json={"status": "success", "data": {"result": [{"value": [0, "1"]}]}},
            )
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collector = SystemCollector(
        hub,
        ingestion_url=settings.ingestion_url,
        features_url=settings.features_url,
        detection_url=settings.detection_url,
        prometheus_url=settings.prometheus_url,
        probe_client=client,
        pool=None,
    )
    try:
        await collector.tick_prom()
        sub_a = hub.subscribe()
        sub_b = hub.subscribe()
        try:
            await anext(sub_a)
            await anext(sub_b)
            before = collector.ready_probe_count
            await collector.tick_ready()
            assert collector.ready_probe_count - before == 3
            assert hub.subscriber_count == 2
        finally:
            await sub_a.aclose()
            await sub_b.aclose()
    finally:
        await collector.aclose()


async def test_collector_prometheus_down_nulls_lag() -> None:
    settings = make_settings()
    hub = SystemHub()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/ready"):
            return httpx.Response(
                200,
                json={"status": "ready", "kafka": "ok", "postgres": "ok", "redis": "ok"},
            )
        if "/api/v1/query" in str(request.url):
            raise httpx.ConnectError("prom down", request=request)
        return httpx.Response(404)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    collector = SystemCollector(
        hub,
        ingestion_url=settings.ingestion_url,
        features_url=settings.features_url,
        detection_url=settings.detection_url,
        prometheus_url=settings.prometheus_url,
        probe_client=client,
        pool=None,
    )
    try:
        await collector.tick_prom()
        event = await collector.tick_ready()
        assert event.mode == "normal"
        by_name = {item.name: item for item in event.components}
        assert by_name["ingestion"].status == "up"
        assert by_name["features-worker"].lag is None
        assert by_name["features-offline"].lag is None
        assert by_name["features-worker"].status == "unknown"
    finally:
        await collector.aclose()
