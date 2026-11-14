from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from uuid import UUID, uuid4

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

POSTGRES_IMAGE = "postgres:17"
INIT_SQL = Path(__file__).resolve().parents[3] / "infra" / "postgres" / "init.sql"


@pytest.fixture(scope="module")
def postgres_conn() -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as postgres:
        with psycopg.connect(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(postgres.port),
            user=postgres.username,
            password=postgres.password,
            dbname=postgres.dbname,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(INIT_SQL.read_text())
            conn.commit()
            yield conn


@pytest.fixture
def conn(
    postgres_conn: psycopg.Connection[tuple[object, ...]],
) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    postgres_conn.rollback()
    with postgres_conn.cursor() as cur:
        cur.execute("TRUNCATE ingestion.idempotency_keys")
    postgres_conn.commit()
    yield postgres_conn


@pytest.mark.integration
def test_duplicate_event_id_inserts_once(conn: psycopg.Connection[tuple[object, ...]]) -> None:
    event_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion.idempotency_keys (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (event_id,),
        )
        cur.execute(
            "INSERT INTO ingestion.idempotency_keys (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (event_id,),
        )
        cur.execute("SELECT count(*) FROM ingestion.idempotency_keys")
        assert cur.fetchone() == (1,)
    conn.commit()


@pytest.mark.integration
def test_cleanup_drops_keys_older_than_25h(conn: psycopg.Connection[tuple[object, ...]]) -> None:
    old_id = uuid4()
    new_id = uuid4()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingestion.idempotency_keys (event_id, received_at)
            VALUES (%s, now() - interval '26 hours'),
                   (%s, now() - interval '1 hour')
            """,
            (old_id, new_id),
        )
        cur.execute("SELECT ingestion.delete_expired_idempotency_keys()")
        cur.execute("SELECT event_id FROM ingestion.idempotency_keys")
        remaining: list[UUID] = [row[0] for row in cur.fetchall()]
    conn.commit()
    assert remaining == [new_id]
