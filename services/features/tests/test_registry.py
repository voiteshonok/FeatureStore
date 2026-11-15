"""Registry upsert against Postgres."""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool
from testcontainers.community.postgres import PostgresContainer

from features.api import upsert_catalog
from schemas import CATALOG

pytestmark = pytest.mark.integration

_POSTGRES = Path(__file__).resolve().parents[3] / "infra" / "postgres"
_POSTGRES_IMAGE = "postgres:17"
_ROLES_SQL = """
CREATE ROLE svc_ingestion LOGIN PASSWORD 'svc_ingestion_dev';
CREATE ROLE svc_features LOGIN PASSWORD 'svc_features_dev';
CREATE ROLE svc_detection LOGIN PASSWORD 'svc_detection_dev';
CREATE ROLE svc_gateway LOGIN PASSWORD 'svc_gateway_dev';
"""
_ROWS = """
SELECT d.name, d.entity, d.kind, d.agg, d.default_value,
       v.version, v.window_ms, v.stale_after_ms
FROM features.feature_definitions AS d
JOIN features.feature_versions AS v USING (name)
ORDER BY d.name
"""


def _dsn(postgres: PostgresContainer, *, user: str, password: str) -> str:
    return psycopg.conninfo.make_conninfo(
        host=postgres.get_container_host_ip(),
        port=postgres.get_exposed_port(postgres.port),
        user=user,
        password=password,
        dbname=postgres.dbname,
    )


def _apply(postgres: PostgresContainer) -> None:
    with psycopg.connect(
        _dsn(postgres, user=postgres.username, password=postgres.password),
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(_ROLES_SQL)
            cur.execute((_POSTGRES / "10-schema.sql").read_text())
            cur.execute((_POSTGRES / "20-seed.sql").read_text())


def _millis(value: object) -> int | None:
    if value is None:
        return None
    total = getattr(value, "total_seconds", None)
    if total is None:
        return None
    return int(total() * 1000)


async def _projection(pool: AsyncConnectionPool) -> list[tuple[object, ...]]:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(_ROWS)
        rows = await cur.fetchall()
    return list(rows)


async def test_registry_upsert_is_unchanged_on_second_boot() -> None:
    with PostgresContainer(_POSTGRES_IMAGE, driver=None) as postgres:
        _apply(postgres)
        pool: AsyncConnectionPool = AsyncConnectionPool(
            _dsn(postgres, user="svc_features", password="svc_features_dev"),  # noqa: S106
            open=False,
        )
        await pool.open(wait=True, timeout=30)
        try:
            await upsert_catalog(pool)
            first = await _projection(pool)
            await upsert_catalog(pool)
            second = await _projection(pool)
        finally:
            await pool.close()

    assert first == second
    assert len(first) == len(CATALOG)
    by_name = {row[0]: row for row in first}
    for spec in CATALOG:
        name, entity, kind, agg, default_value, version, window_ms, stale_ms = by_name[spec.name]
        assert entity == spec.entity
        assert kind == spec.kind
        assert agg == spec.agg
        assert version == 1
        assert window_ms == _millis(spec.window)
        assert stale_ms == _millis(spec.stale_after)
        if spec.default is None:
            assert default_value is None
        else:
            assert default_value == float(spec.default)
