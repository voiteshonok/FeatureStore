"""Share Kafka and Postgres containers between detection integration tests."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from detection_stack import (  # noqa: E402
    POSTGRES_IMAGE,
    KafkaService,
    ensure_topic,
    kafka_container,
    unused_port,
    wait_for_kafka,
)

_INIT_SQL = Path(__file__).resolve().parents[3] / "infra" / "postgres" / "init.sql"


def _dsn(postgres: PostgresContainer, *, user: str, password: str) -> str:
    return psycopg.conninfo.make_conninfo(
        host=postgres.get_container_host_ip(),
        port=postgres.get_exposed_port(postgres.port),
        user=user,
        password=password,
        dbname=postgres.dbname,
    )


def _apply(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(_INIT_SQL.read_text())


@dataclass(frozen=True)
class Database:
    superuser_dsn: str
    service_dsn: str


@pytest.fixture(scope="module")
def kafka() -> Iterator[KafkaService]:
    host_port = unused_port()
    bootstrap = f"127.0.0.1:{host_port}"
    container = kafka_container(host_port)
    with container:
        wait_for_kafka(bootstrap)
        ensure_topic(bootstrap)
        yield KafkaService(container, bootstrap)


@pytest.fixture(scope="module")
def database() -> Iterator[Database]:
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as postgres:
        superuser = _dsn(postgres, user=postgres.username, password=postgres.password)
        _apply(superuser)
        service = _dsn(
            postgres,
            user="svc_detection",
            password="featurestore",  # noqa: S106
        )
        yield Database(superuser, service)
