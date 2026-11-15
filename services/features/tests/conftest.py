"""Put the tests directory on the path and share Kafka and Redis containers."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from testcontainers.community.redis import RedisContainer

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from stack import (  # noqa: E402
    REDIS_IMAGE,
    KafkaService,
    ensure_topics,
    kafka_container,
    unused_port,
    wait_for_kafka,
)


@pytest.fixture(scope="module")
def kafka() -> Iterator[KafkaService]:
    host_port = unused_port()
    bootstrap = f"127.0.0.1:{host_port}"
    container = kafka_container(host_port)
    with container:
        wait_for_kafka(bootstrap)
        ensure_topics(bootstrap)
        yield KafkaService(container, bootstrap)


@pytest.fixture(scope="module")
def redis_url() -> Iterator[str]:
    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"
