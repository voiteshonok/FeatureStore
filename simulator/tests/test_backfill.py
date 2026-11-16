from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from itertools import islice

import httpx
import pytest

from simulator.cli import main
from simulator.config import SimulatorConfig, load_config
from simulator.generator import GeneratedPayment, StepClock, backfill_clock, generate
from simulator.sender import run_backfill

_PRIMARY = frozenset({"normal", "late_short", "late_long"})
_T0 = datetime(2026, 6, 1, tzinfo=UTC)


def _history(
    seed: int,
    run_id: str,
    depth: timedelta,
    tps: float,
) -> tuple[list[GeneratedPayment], datetime, datetime]:
    clock, horizon, floor = backfill_clock(_T0, depth, tps)
    items = list(generate(load_config(), seed, clock, run_id=run_id, horizon=horizon, floor=floor))
    return items, horizon, floor


def test_backfill_stream_is_stable_and_inside_the_horizon() -> None:
    depth = timedelta(seconds=2)
    tps = 10
    first, horizon, floor = _history(3, "run-a", depth, tps)
    second, _, _ = _history(3, "run-a", depth, tps)
    assert first == second
    primaries = [item for item in first if item.kind in _PRIMARY]
    assert len(primaries) == int(depth.total_seconds() * tps)
    for item in first:
        assert floor <= item.payment.event_time <= horizon
    for previous, item in zip(first, first[1:], strict=False):
        assert previous.send_at <= item.send_at


def test_open_generate_ignores_an_unset_horizon() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    step = timedelta(milliseconds=5)
    config = load_config()
    open_stream = list(islice(generate(config, 1, StepClock(start, step)), 100))
    same = list(islice(generate(config, 1, StepClock(start, step), horizon=None, floor=None), 100))
    assert open_stream == same


def test_late_events_are_clamped_to_the_floor() -> None:
    data = load_config().model_dump()
    data["late_long_rate"] = 1
    data["late_short_rate"] = 0
    config = SimulatorConfig.model_validate(data)
    depth = timedelta(seconds=2)
    clock, horizon, floor = backfill_clock(_T0, depth, 10)
    items = list(generate(config, 5, clock, run_id="late", horizon=horizon, floor=floor))
    primaries = [item for item in items if item.kind in _PRIMARY]
    assert primaries
    assert all(item.kind == "late_long" for item in primaries)
    assert all(item.payment.event_time == floor for item in primaries)


def test_cli_rejects_a_day_or_a_send_rate_on_its_own() -> None:
    with pytest.raises(SystemExit):
        main(
            [
                "--tps",
                "10",
                "--seed",
                "1",
                "--url",
                "http://localhost:8000",
                "--backfill",
                "24h",
                "--send-rate",
                "10",
            ]
        )
    with pytest.raises(SystemExit):
        main(
            [
                "--tps",
                "10",
                "--seed",
                "1",
                "--url",
                "http://localhost:8000",
                "--duration",
                "1s",
                "--send-rate",
                "10",
            ]
        )
    with pytest.raises(SystemExit):
        main(
            [
                "--tps",
                "10",
                "--seed",
                "1",
                "--url",
                "http://localhost:8000",
                "--backfill",
                "1h",
                "--duration",
                "1s",
                "--send-rate",
                "10",
            ]
        )


async def test_in_flight_posts_stay_within_the_semaphore() -> None:
    release = asyncio.Event()
    in_flight = 0
    peak = 0

    async def app(
        scope: dict[str, object],
        receive: object,
        send: object,
    ) -> None:
        nonlocal in_flight, peak
        if scope["type"] != "http":
            return
        assert callable(receive)
        assert callable(send)
        while True:
            message = await receive()
            if message["type"] == "http.request" and not message.get("more_body", False):
                break
        in_flight += 1
        peak = max(peak, in_flight)
        await release.wait()
        in_flight -= 1
        await send(
            {
                "type": "http.response.start",
                "status": 202,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b'{"duplicate": false}'})

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        task = asyncio.create_task(
            run_backfill(
                url="http://test",
                tps=20,
                depth_s=1,
                send_rate=1_000,
                seed=1,
                run_id="cap",
                max_in_flight=2,
                t0=_T0,
                client=client,
                report=False,
            )
        )
        for _ in range(200):
            if peak >= 2:
                break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        assert peak == 2
        release.set()
        stats = await task
    assert stats.http_422 == 0
    payload = stats.to_json()
    assert "event_time_min" in payload
    assert "event_time_max" in payload
    assert stats.generated >= 20
