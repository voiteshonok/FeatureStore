"""Format SSE frames for the system state stream (ADR-022)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator

from gateway.system.hub import StreamMessage, SystemHub

_PING_S = 15.0
_WAIT_S = 1.0
_RETRY_MS = 3000


async def sse_frames(hub: SystemHub) -> AsyncGenerator[bytes, None]:
    """Yield SSE bytes: retry, then events, with ping comments every 15 s.

    ``wait_for`` must not cancel ``anext`` on timeout — that would close the
    subscriber generator and end the stream after the first quiet second.
    """
    yield f"retry: {_RETRY_MS}\n\n".encode()
    stream = hub.subscribe()
    pending: asyncio.Task[StreamMessage] = asyncio.create_task(anext(stream))
    idle = 0.0
    try:
        while True:
            try:
                message = await asyncio.wait_for(asyncio.shield(pending), timeout=_WAIT_S)
            except TimeoutError:
                idle += _WAIT_S
                if idle >= _PING_S:
                    yield b": ping\n\n"
                    idle = 0.0
                continue
            except StopAsyncIteration:
                return
            idle = 0.0
            yield _encode(message)
            pending = asyncio.create_task(anext(stream))
    finally:
        pending.cancel()
        try:
            await pending
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        await stream.aclose()


def _encode(message: StreamMessage) -> bytes:
    lines = [f"event: {message.event}"]
    payload = json.dumps(message.data, separators=(",", ":"), default=str)
    for part in payload.split("\n"):
        lines.append(f"data: {part}")
    lines.append("")
    return ("\n".join(lines) + "\n").encode()
