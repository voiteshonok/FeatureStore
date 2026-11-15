from __future__ import annotations

import asyncio
import random

import pytest
from aiokafka import AIOKafkaProducer
from containers import KafkaService

from ingestion.publisher import TOPIC
from schemas import partition_for

pytestmark = pytest.mark.integration

_KEY_COUNT = 1000
_BATCH = 100


def _keys(count: int) -> list[str]:
    rng = random.Random(23)  # noqa: S311
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    keys: list[str] = []
    for index in range(count):
        body = "".join(rng.choice(alphabet) for _ in range(1 + index % 40))
        if index % 100 == 0:
            body += "é"
        keys.append(body)
    return keys


async def test_partition_for_matches_producer(kafka: KafkaService) -> None:
    producer = AIOKafkaProducer(bootstrap_servers=kafka.bootstrap)
    await producer.start()
    try:
        partitions = await producer.partitions_for(TOPIC)
        n_partitions = len(partitions)
        assert partitions == set(range(n_partitions))
        keys = _keys(_KEY_COUNT)

        async def published_partition(key: str) -> None:
            metadata = await producer.send_and_wait(
                TOPIC,
                value=b"x",
                key=key.encode("utf-8"),
            )
            assert metadata.partition == partition_for(key, n_partitions)

        for start in range(0, _KEY_COUNT, _BATCH):
            await asyncio.gather(
                *(published_partition(key) for key in keys[start : start + _BATCH])
            )
    finally:
        await producer.stop()
