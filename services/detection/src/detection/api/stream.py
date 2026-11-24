"""SSE endpoint for live alerts and stats (ADR-021)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Header, Request
from fastapi.responses import StreamingResponse

from detection.api.examples import ALERT_SSE_EXAMPLE, STREAM_DESCRIPTION
from detection.live.broadcast import SubscriberFull
from detection.live.sse import sse_frames
from schemas import AppProblem

router = APIRouter(tags=["stream"])


@router.get(
    "/v1/stream/alerts",
    summary="Alert stream",
    description=STREAM_DESCRIPTION,
    openapi_extra={"security": []},
    responses={
        200: {
            "description": "SSE stream. See docs/asyncapi/alerts-stream.yaml.",
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                    "example": ALERT_SSE_EXAMPLE,
                }
            },
        }
    },
)
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
