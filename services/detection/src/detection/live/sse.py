"""Format SSE frames for the alert stream (ADR-021)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from detection.live.broadcast import BroadcastHub, StreamMessage, SubscriberFull

_RETRY_MS = 3000
_PING_S = 15.0


async def sse_frames(
    hub: BroadcastHub,
    *,
    last_event_id: int | None,
) -> AsyncIterator[bytes]:
    """Yield SSE bytes: retry, then events, with ping comments every 15 s."""
    yield f"retry: {_RETRY_MS}\n\n".encode()
    stream = hub.subscribe(last_event_id)
    try:
        while True:
            try:
                message = await asyncio.wait_for(anext(stream), timeout=_PING_S)
            except TimeoutError:
                yield b": ping\n\n"
                continue
            except StopAsyncIteration:
                return
            yield _encode(message)
    finally:
        await stream.aclose()


def _encode(message: StreamMessage) -> bytes:
    lines = [f"event: {message.event}"]
    if message.event_id is not None:
        lines.append(f"id: {message.event_id}")
    payload = json.dumps(message.data, separators=(",", ":"), default=str)
    for part in payload.split("\n"):
        lines.append(f"data: {part}")
    lines.append("")
    return ("\n".join(lines) + "\n").encode()


__all__ = ["SubscriberFull", "sse_frames"]
