"""Backfill a short history and compare the hour count with the log.

Requires ``make secret`` and ``make up``. The history is 15 s at 50 TPS, about
750 events. That stays inside the one-hour window, so the test checks parity
and a zero 422 count without waiting for the writer to apply tens of thousands
of events. ``card_cnt_24h`` exceeding ``card_cnt_1h`` is the check for a warmup
deeper than one hour (``make backfill BACKFILL=23h``).
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from test_stage3_features import (  # noqa: E402
    _API,
    _BOOTSTRAP,
    _HOUR,
    _INGESTION,
    _PARTITIONS,
    _TOPIC,
    _card_sum,
    _caught_up,
    _features,
    _group_offsets,
    _post,
    _read_events,
)

from simulator.sender import run_backfill  # noqa: E402

pytestmark = pytest.mark.e2e


async def _wait_for_online_writer() -> None:
    consumer = AIOKafkaConsumer(bootstrap_servers=_BOOTSTRAP, enable_auto_commit=False)
    partitions = [TopicPartition(_TOPIC, index) for index in range(_PARTITIONS)]
    consumer.assign(partitions)
    await consumer.start()
    try:
        # The writer applies one event at a time. This test's history is small.
        deadline = time.monotonic() + 5 * 60
        while time.monotonic() < deadline:
            ends = await consumer.end_offsets(partitions)
            end_map = {tp.partition: int(ends[tp]) for tp in partitions}
            online = await asyncio.to_thread(_group_offsets, "online-writer")
            if _caught_up(online, end_map):
                return
            await asyncio.sleep(0.5)
    finally:
        await consumer.stop()
    pytest.fail("online-writer did not reach lag 0")


async def test_short_backfill_matches_the_hour_count() -> None:
    stats = await run_backfill(
        url=_INGESTION,
        tps=50,
        depth_s=15,
        send_rate=200,
        seed=11,
        run_id=str(uuid4()),
        report=False,
    )
    assert stats.http_422 == 0
    assert stats.generated > 0
    await _wait_for_online_writer()
    events = await _read_events()
    by_card: dict[str, list] = defaultdict(list)
    for event in events:
        if event.lat is None or event.lon is None:
            continue
        by_card[event.card_token].append(event)
    ranked = sorted(by_card, key=lambda token: len(by_card[token]), reverse=True)[:20]
    assert ranked
    async with httpx.AsyncClient(base_url=_API, timeout=10) as client:
        for token in ranked:
            card_events = by_card[token]
            latest = max(card_events, key=lambda event: event.event_time)
            assert latest.lat is not None and latest.lon is not None
            deadline = time.monotonic() + 20
            matched = False
            while time.monotonic() < deadline:
                now = datetime.now(UTC)
                body = await _post(
                    client,
                    card_token=token,
                    merchant_id=latest.merchant_id,
                    lat=latest.lat,
                    lon=latest.lon,
                )
                count, _sum = _card_sum(card_events, now, _HOUR)
                if _features(body)["card_cnt_1h"]["value"] == count:
                    matched = True
                    break
                await asyncio.sleep(0.5)
            assert matched, token
