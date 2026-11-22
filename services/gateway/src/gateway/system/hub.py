"""Fan-out hub for system-state SSE subscribers (ADR-022)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any, Literal

from schemas import SystemStreamEvent

MAX_SUBSCRIBERS = 200
QUEUE_SIZE = 100

EventName = Literal["snapshot", "change"]


class SubscriberFull(Exception):
    """Too many live subscribers."""


@dataclass(frozen=True)
class StreamMessage:
    """One SSE payload waiting in a subscriber queue."""

    event: EventName
    data: dict[str, Any]


class SystemHub:
    """Latest snapshot plus per-subscriber queues."""

    def __init__(
        self,
        *,
        max_subscribers: int = MAX_SUBSCRIBERS,
        queue_size: int = QUEUE_SIZE,
    ) -> None:
        self._max_subscribers = max_subscribers
        self._queue_size = queue_size
        self._subscribers: dict[int, asyncio.Queue[StreamMessage | None]] = {}
        self._next_sub_id = 1
        self._latest: SystemStreamEvent | None = None
        self._lock = asyncio.Lock()

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def is_full(self) -> bool:
        return len(self._subscribers) >= self._max_subscribers

    @property
    def latest(self) -> SystemStreamEvent | None:
        return self._latest

    async def subscribe(self) -> AsyncGenerator[StreamMessage, None]:
        """Yield snapshot then changes until disconnect or overflow."""
        async with self._lock:
            if len(self._subscribers) >= self._max_subscribers:
                raise SubscriberFull
            sub_id = self._next_sub_id
            self._next_sub_id += 1
            queue: asyncio.Queue[StreamMessage | None] = asyncio.Queue(maxsize=self._queue_size)
            self._subscribers[sub_id] = queue
            latest = self._latest
        if latest is not None:
            await queue.put(
                StreamMessage(
                    event="snapshot",
                    data=latest.model_dump(mode="json"),
                )
            )
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            async with self._lock:
                self._subscribers.pop(sub_id, None)

    async def publish(self, event: EventName, snapshot: SystemStreamEvent) -> None:
        """Store latest and fan out. ``snapshot`` on first publish, else ``change``."""
        message = StreamMessage(event=event, data=snapshot.model_dump(mode="json"))
        async with self._lock:
            self._latest = snapshot
            await self._fanout(message)

    async def _fanout(self, message: StreamMessage) -> None:
        dead: list[int] = []
        for sub_id, queue in self._subscribers.items():
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(sub_id)
        for sub_id in dead:
            if sub_id not in self._subscribers:
                continue
            queue = self._subscribers[sub_id]
            del self._subscribers[sub_id]
            try:
                queue.put_nowait(None)
            except asyncio.QueueFull:
                pass
