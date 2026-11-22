"""SSE endpoint for live alerts and stats (ADR-021)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse

from detection.live.broadcast import SubscriberFull
from detection.live.sse import sse_frames
from schemas import AppProblem

router = APIRouter(tags=["stream"])


@router.get("/v1/stream/alerts")
async def stream_alerts(
    request: Request,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> StreamingResponse:
    """Push alert/stats events until the client disconnects."""
    hub = request.app.state.broadcast
    if hub.is_full:
        raise AppProblem("upstream_unavailable", "too many stream subscribers", 503)
    parsed: int | None = None
    if last_event_id is not None and last_event_id.strip() != "":
        try:
            parsed = int(last_event_id.strip())
        except ValueError:
            parsed = None

    async def frames() -> AsyncIterator[bytes]:
        try:
            async for chunk in sse_frames(hub, last_event_id=parsed):
                if await request.is_disconnected():
                    break
                yield chunk
        except SubscriberFull:
            return

    return StreamingResponse(
        frames(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
