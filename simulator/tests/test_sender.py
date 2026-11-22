from __future__ import annotations

import asyncio
import socket
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from schemas import PaymentIn
from simulator.sender import Stats, parse_duration, post_payment, run_sender

_INGESTION_TESTS = Path(__file__).resolve().parents[2] / "services" / "ingestion" / "tests"

_KEYS = {
    "generated",
    "sent",
    "accepted",
    "duplicates",
    "http_422",
    "http_503",
    "retries",
    "failed",
    "achieved_tps",
    "latency_p50_ms",
    "latency_p95_ms",
    "elapsed_s",
    "anomalies",
}


def _payment() -> PaymentIn:
    now = datetime.now(UTC).replace(microsecond=0)
    return PaymentIn.model_validate(
        {
            "event_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
            "payment_id": "pay-1",
            "pan": "4111111111111111",
            "merchant_id": "m-1",
            "amount_minor": 100,
            "currency": "EUR",
            "status": "approved",
            "event_time": now,
            "lat": 52.52,
            "lon": 13.405,
        }
    )


def test_parse_duration() -> None:
    assert parse_duration("60") == 60
    assert parse_duration("60s") == 60
    assert parse_duration("10m") == 600
    assert parse_duration("1h") == 3600
    with pytest.raises(ValueError):
        parse_duration("0s")


def test_stats_json_keys() -> None:
    stats = Stats(generated=4, accepted=3, duplicates=1, elapsed_s=2)
    stats.latencies_ms.extend([1.0, 2.0, 3.0, 4.0])
    payload = stats.to_json()
    assert set(payload) == _KEYS
    assert payload["achieved_tps"] == 2
    assert payload["anomalies"] == {"burst": 0, "bad_merchant": 0, "travel": 0}


async def test_accepted_and_duplicate_are_counted() -> None:
    bodies = iter(({"duplicate": False}, {"duplicate": True}))

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json=next(bodies))

    stats = Stats()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_payment(client, "http://test/v1/events", _payment(), stats, asyncio.Lock())
        await post_payment(client, "http://test/v1/events", _payment(), stats, asyncio.Lock())
    assert stats.accepted == 1
    assert stats.duplicates == 1
    assert stats.sent == 2
    assert stats.retries == 0


async def test_422_is_not_retried() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(422, json={"detail": []})

    stats = Stats()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_payment(client, "http://test/v1/events", _payment(), stats, asyncio.Lock())
    assert calls == 1
    assert stats.http_422 == 1
    assert stats.retries == 0
    assert stats.failed == 0


async def test_503_retries_then_accepts() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, headers={"Retry-After": "0"}, json={"detail": "down"})
        return httpx.Response(202, json={"duplicate": False})

    stats = Stats()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_payment(client, "http://test/v1/events", _payment(), stats, asyncio.Lock())
    assert calls == 2
    assert stats.retries == 1
    assert stats.http_503 == 1
    assert stats.accepted == 1
    assert stats.failed == 0


async def test_connection_error_retries_then_accepts() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("down")
        return httpx.Response(202, json={"duplicate": False})

    stats = Stats()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_payment(client, "http://test/v1/events", _payment(), stats, asyncio.Lock())
    assert calls == 2
    assert stats.retries == 1
    assert stats.accepted == 1
    assert stats.sent == 2


async def test_five_failures_are_given_up() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, headers={"Retry-After": "0"}, json={"detail": "down"})

    stats = Stats()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await post_payment(client, "http://test/v1/events", _payment(), stats, asyncio.Lock())
    assert calls == 5
    assert stats.retries == 4
    assert stats.http_503 == 5
    assert stats.failed == 1
    assert stats.accepted == 0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _serve(app: FastAPI) -> Iterator[str]:
    import uvicorn

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    )
    server.install_signal_handlers = lambda: None
    task = asyncio.create_task(server.serve())
    try:
        for _ in range(100):
            if server.started:
                break
            await asyncio.sleep(0.05)
        else:
            msg = "server did not start"
            raise RuntimeError(msg)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@pytest.mark.integration
async def test_rate_stays_within_ten_percent_for_60s() -> None:
    app = FastAPI(
        telemetry={
            "tracing": False,
            "metrics": False,
            "logs": False,
            "operation_spans": False,
        }
    )

    @app.post("/v1/events")
    async def accept() -> JSONResponse:
        return JSONResponse({"event_id": "x", "duplicate": False}, status_code=202)

    async for base in _serve(app):
        stats = await run_sender(url=base, tps=200, duration_s=60, seed=1, report=False)
        assert stats.failed == 0
        assert stats.accepted == stats.generated
        # The sender and this stub share one event loop. A busy runner admits
        # a little under the token-bucket target; CI has landed near 187/s.
        assert abs(stats.achieved_tps - 200) <= 200 * 0.10


@pytest.mark.integration
async def test_kafka_outage_loses_no_events(tmp_path: Path) -> None:
    if str(_INGESTION_TESTS) not in sys.path:
        sys.path.insert(0, str(_INGESTION_TESTS))
    from containers import (
        KafkaService,
        apply_schema,
        connection_dsn,
        kafka_container,
        unused_port,
        wait_for_kafka,
    )
    from testcontainers.community.postgres import PostgresContainer

    from ingestion.app import Settings, create_app

    host_port = unused_port()
    bootstrap = f"127.0.0.1:{host_port}"
    container = kafka_container(host_port)
    with container:
        wait_for_kafka(bootstrap)
        kafka = KafkaService(container, bootstrap)
        with PostgresContainer("postgres:17", driver=None) as postgres:
            dsn = connection_dsn(postgres)
            apply_schema(dsn)
            key_path = tmp_path / "hmac_key"
            key_path.write_text(bytes(range(32)).hex() + "\n", encoding="utf-8")
            app = create_app(
                Settings(
                    kafka_bootstrap=bootstrap,
                    database_url=dsn,
                    hmac_key_path=str(key_path),
                    cleanup_interval_s=3600,
                    ack_timeout_s=10,
                )
            )
            async for base in _serve(app):

                async def outage() -> None:
                    await asyncio.sleep(20)
                    await asyncio.to_thread(kafka.stop)
                    try:
                        await asyncio.sleep(30)
                    finally:
                        await asyncio.to_thread(kafka.start)
                        await asyncio.to_thread(wait_for_kafka, bootstrap)

                stats, _ = await asyncio.gather(
                    run_sender(url=base, tps=1, duration_s=70, seed=4, report=False),
                    outage(),
                )
    assert stats.failed == 0
    assert stats.accepted + stats.duplicates + stats.http_422 == stats.generated
