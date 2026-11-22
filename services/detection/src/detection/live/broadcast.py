"""Fan-out hub for SSE subscribers (ADR-021)."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any, Literal

from schemas import AlertStreamEvent, ResetStreamEvent, StatsStreamEvent

MAX_SUBSCRIBERS = 200
QUEUE_SIZE = 100
BUFFER_SIZE = 500

EventName = Literal["alert", "stats", "reset"]


class SubscriberFull(Exception):
    """Too many live subscribers."""


@dataclass(frozen=True)
class StreamMessage:
    """One SSE payload waiting in a subscriber queue."""

    event: EventName
    data: dict[str, Any]
    event_id: int | None = None


class BroadcastHub:
    """Ring buffer of alert events plus per-subscriber queues."""

    def __init__(
        self,
        *,
        max_subscribers: int = MAX_SUBSCRIBERS,
        queue_size: int = QUEUE_SIZE,
        buffer_size: int = BUFFER_SIZE,
        on_subscribers_changed: Any | None = None,
        on_dropped: Any | None = None,
    ) -> None:
        self._max_subscribers = max_subscribers
        self._queue_size = queue_size
        self._buffer_size = buffer_size
        self._on_subscribers_changed = on_subscribers_changed
        self._on_dropped = on_dropped
        self._subscribers: dict[int, asyncio.Queue[StreamMessage | None]] = {}
        self._next_sub_id = 1
        self._next_event_id = 1
        self._buffer: deque[StreamMessage] = deque(maxlen=buffer_size)
        self._lock = asyncio.Lock()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def is_full(self) -> bool:
        return len(self._subscribers) >= self._max_subscribers

    async def subscribe(
        self,
        last_event_id: int | None = None,
    ) -> AsyncGenerator[StreamMessage, None]:
        """Yield messages until the client disconnects or the queue overflows."""
        async with self._lock:
            if len(self._subscribers) >= self._max_subscribers:
                raise SubscriberFull
            sub_id = self._next_sub_id
            self._next_sub_id += 1
            queue: asyncio.Queue[StreamMessage | None] = asyncio.Queue(maxsize=self._queue_size)
            self._subscribers[sub_id] = queue
            replay = self._replay_messages(last_event_id)
            if self._on_subscribers_changed is not None:
                self._on_subscribers_changed(len(self._subscribers))
        for message in replay:
            await queue.put(message)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            async with self._lock:
                self._subscribers.pop(sub_id, None)
                if self._on_subscribers_changed is not None:
                    self._on_subscribers_changed(len(self._subscribers))

    def _replay_messages(self, last_event_id: int | None) -> list[StreamMessage]:
        if last_event_id is None:
            return []
        if not self._buffer:
            return [
                StreamMessage(
                    event="reset",
                    data=ResetStreamEvent(reason="gap").model_dump(mode="json"),
                )
            ]
        oldest = self._buffer[0].event_id
        newest = self._buffer[-1].event_id
        if oldest is None or newest is None:
            return [
                StreamMessage(
                    event="reset",
                    data=ResetStreamEvent(reason="gap").model_dump(mode="json"),
                )
            ]
        if last_event_id < oldest - 1 or last_event_id > newest:
            return [
                StreamMessage(
                    event="reset",
                    data=ResetStreamEvent(reason="gap").model_dump(mode="json"),
                )
            ]
        return [
            msg for msg in self._buffer if msg.event_id is not None and msg.event_id > last_event_id
        ]

    async def publish_alert(self, event: AlertStreamEvent) -> int:
        """Assign a stream id, buffer, and fan out. Returns the assigned id."""
        async with self._lock:
            event_id = self._next_event_id
            self._next_event_id += 1
            payload = event.model_copy(update={"id": event_id})
            message = StreamMessage(
                event="alert",
                data=payload.model_dump(mode="json"),
                event_id=event_id,
            )
            self._buffer.append(message)
            await self._fanout(message)
            return event_id

    async def publish_stats(self, event: StatsStreamEvent) -> None:
        message = StreamMessage(
            event="stats",
            data=event.model_dump(mode="json"),
        )
        async with self._lock:
            await self._fanout(message)

    async def publish_reset(self, *, reason: Literal["gap", "reconnect"] = "reconnect") -> None:
        message = StreamMessage(
            event="reset",
            data=ResetStreamEvent(reason=reason).model_dump(mode="json"),
        )
        async with self._lock:
            self._buffer.clear()
            self._next_event_id = 1
            await self._fanout(message)

    async def _fanout(self, message: StreamMessage) -> None:
        dead: list[int] = []
        for sub_id, queue in self._subscribers.items():
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(sub_id)
                if self._on_dropped is not None:
                    self._on_dropped()
        for sub_id in dead:
            if sub_id not in self._subscribers:
                continue
            queue = self._subscribers[sub_id]
            del self._subscribers[sub_id]
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
            if self._on_subscribers_changed is not None:
                self._on_subscribers_changed(len(self._subscribers))
