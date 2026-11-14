from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from testcontainers.community.postgres import PostgresContainer

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from containers import (  # noqa: E402
    POSTGRES_IMAGE,
    KafkaService,
    apply_schema,
    connection_dsn,
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
        yield KafkaService(container, bootstrap)


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as postgres:
        dsn = connection_dsn(postgres)
        apply_schema(dsn)
        yield dsn
