"""Stage 7.5 acceptance: system SSE reflects Redis stop within 10 s (US-6.2).

Requires ``make secret`` and ``make up`` with prometheus in the default stack.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import time
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.e2e

_GATEWAY_URL = "http://localhost:8080"
_COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml"]
_REPO = Path(__file__).resolve().parents[2]
_LOGIN_EMAIL = "viewer@featurestore.local"
_LOGIN_PASSWORD = "demo-viewer-pass"  # noqa: S105 — seed password from 20-seed.sql
_DEADLINE_S = 10.0


def _compose(*args: str) -> None:
    subprocess.run(  # noqa: S603 — fixed compose argv, no shell
        [*_COMPOSE, *args],
        cwd=_REPO,
        check=True,
        capture_output=True,
        text=True,
    )


async def _login(client: httpx.AsyncClient) -> dict[str, str]:
    """Return Cookie headers for gateway routes on :8080.

    The session cookie is Path=/api for the dashboard nginx proxy. Direct hits
    to ``/v1/...`` on the gateway port do not match that path, so the jar would
    drop the cookie — send it explicitly instead.
    """
    response = await client.post(
        f"{_GATEWAY_URL}/v1/auth/login",
        json={"email": _LOGIN_EMAIL, "password": _LOGIN_PASSWORD},
    )
    if response.status_code != 204:
        pytest.fail(f"login failed: {response.status_code} {response.text}")
    token = response.cookies.get("fs_session")
    if not token:
        pytest.fail("login succeeded but fs_session cookie is missing")
    return {"Cookie": f"fs_session={token}"}


def _parse_sse(buffer: str) -> list[tuple[str, dict[str, object]]]:
    events: list[tuple[str, dict[str, object]]] = []
    for block in buffer.split("\n\n"):
        event_name: str | None = None
        data_lines: list[str] = []
        for line in block.split("\n"):
            if line.startswith("event:"):
                event_name = line[len("event:") :].strip()
            elif line.startswith("data:"):
                data_lines.append(line[len("data:") :].strip())
        if event_name is None or not data_lines:
            continue
        payload = json.loads("\n".join(data_lines))
        if isinstance(payload, dict):
            events.append((event_name, payload))
    return events


def _redis_status(payload: dict[str, object]) -> str | None:
    components = payload.get("components")
    if not isinstance(components, list):
        return None
    for item in components:
        if not isinstance(item, dict):
            continue
        if item.get("name") == "Redis":
            status = item.get("status")
            return status if isinstance(status, str) else None
    return None


@pytest.mark.asyncio
async def test_redis_stop_emits_change_within_10s() -> None:
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        auth = await _login(client)
        # Ensure Redis is up before we measure.
        _compose("start", "redis")
        await asyncio.sleep(2)

        async with client.stream(
            "GET",
            f"{_GATEWAY_URL}/v1/stream/system",
            headers=auth,
        ) as response:
            if response.status_code != 200:
                body = await response.aread()
                pytest.fail(f"stream open failed: {response.status_code} {body!r}")

            buffer = ""
            saw_snapshot = False
            chunks = response.aiter_text()
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline and not saw_snapshot:
                try:
                    chunk = await asyncio.wait_for(anext(chunks), timeout=5)
                except (TimeoutError, StopAsyncIteration):
                    break
                buffer += chunk
                for name, payload in _parse_sse(buffer):
                    if name == "snapshot":
                        saw_snapshot = True
                        assert _redis_status(payload) in {"up", "unknown", "down"}
                        break

            assert saw_snapshot, f"no snapshot in {buffer!r}"

            _compose("stop", "redis")
            started = time.monotonic()
            saw_down = False
            while time.monotonic() - started < _DEADLINE_S:
                try:
                    chunk = await asyncio.wait_for(anext(chunks), timeout=2)
                except (TimeoutError, StopAsyncIteration):
                    continue
                buffer += chunk
                for name, payload in _parse_sse(buffer):
                    if name != "change":
                        continue
                    if _redis_status(payload) == "down":
                        saw_down = True
                        assert payload.get("mode") == "degraded"
                        break
                if saw_down:
                    break

            elapsed = time.monotonic() - started
            assert saw_down, f"Redis down not seen in {elapsed:.1f}s; buffer={buffer[-2000:]!r}"
            assert elapsed <= _DEADLINE_S

    # Restore Redis for later tests / the running stack.
    _compose("start", "redis")
