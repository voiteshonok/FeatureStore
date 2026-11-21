"""Broadcast hub: buffer, Last-Event-ID, overflow."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from detection.live.broadcast import BroadcastHub, SubscriberFull
from schemas import AlertStreamEvent


def _alert(op: str = "created") -> AlertStreamEvent:
    return AlertStreamEvent(
        id=0,
        op=op,  # type: ignore[arg-type]
        alert_id=uuid4(),
        rule_id=uuid4(),
        rule_name="card_velocity_burst",
        status="open",
        entity="card",
        entity_id="abcd…ef",
        created_at=datetime.now(UTC),
    )


@pytest.mark.asyncio
async def test_replay_from_last_event_id() -> None:
    hub = BroadcastHub(buffer_size=10)
    await hub.publish_alert(_alert())
    await hub.publish_alert(_alert())

    messages: list[object] = []

    async def collect() -> None:
        async for message in hub.subscribe(last_event_id=1):
            messages.append(message)
            break

    await asyncio.wait_for(collect(), timeout=1)
    assert len(messages) == 1
    assert messages[0].event_id == 2


@pytest.mark.asyncio
async def test_gap_sends_reset() -> None:
    hub = BroadcastHub(buffer_size=2)
    for _ in range(4):
        await hub.publish_alert(_alert())
    # Buffer holds ids 3–4; last_event_id=1 is outside → reset.

    messages: list[object] = []

    async def collect() -> None:
        async for message in hub.subscribe(last_event_id=1):
            messages.append(message)
            break

    await asyncio.wait_for(collect(), timeout=1)
    assert messages[0].event == "reset"  # type: ignore[union-attr]


@pytest.mark.asyncio
async def test_queue_overflow_drops_subscriber() -> None:
    dropped = {"n": 0}
    hub = BroadcastHub(queue_size=1, on_dropped=lambda: dropped.__setitem__("n", 1))
    gate = asyncio.Event()

    async def hold() -> None:
        async for _message in hub.subscribe():
            await gate.wait()
            break

    task = asyncio.create_task(hold())
    await asyncio.sleep(0.05)
    await hub.publish_alert(_alert())
    await asyncio.sleep(0.05)
    await hub.publish_alert(_alert())
    await hub.publish_alert(_alert())
    await asyncio.sleep(0.05)
    assert dropped["n"] == 1
    gate.set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_subscriber_cap() -> None:
    hub = BroadcastHub(max_subscribers=1)
    started = asyncio.Event()

    async def hold() -> None:
        async for _message in hub.subscribe():
            started.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(hold())
    # Publish so the subscriber enters the loop and sets the event... actually
    # subscribe registers before waiting, but the async for hasn't started until
    # the generator runs. Kick with a short sleep after scheduling.
    await asyncio.sleep(0.05)
    # Force registration by publishing after a microtask yield from subscribe start:
    # create a waiter that only sets started once subscribed via callback.
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    registered = asyncio.Event()

    async def hold2() -> None:
        agen = hub.subscribe()
        registered.set()
        async for _message in agen:
            await asyncio.sleep(10)

    task2 = asyncio.create_task(hold2())
    await asyncio.wait_for(registered.wait(), timeout=1)
    with pytest.raises(SubscriberFull):
        async for _ in hub.subscribe():
            break
    task2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task2
