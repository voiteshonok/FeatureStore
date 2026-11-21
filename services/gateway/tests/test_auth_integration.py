"""Postgres login and column-grant isolation (ADR-020)."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pytest
from httpx import ASGITransport, AsyncClient
from testcontainers.community.postgres import PostgresContainer

from gateway.api import create_app
from gateway.settings import Settings

pytestmark = pytest.mark.integration

_POSTGRES = Path(__file__).resolve().parents[3] / "infra" / "postgres"
_POSTGRES_IMAGE = "postgres:17"
_TEST_JWT = bytes.fromhex("cd" * 32)

_ROLES_SQL = """
CREATE ROLE svc_ingestion LOGIN PASSWORD 'svc_ingestion_dev';
CREATE ROLE svc_features LOGIN PASSWORD 'svc_features_dev';
CREATE ROLE svc_detection LOGIN PASSWORD 'svc_detection_dev';
CREATE ROLE svc_gateway LOGIN PASSWORD 'svc_gateway_dev';
"""


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
            cur.execute(_ROLES_SQL)
            cur.execute((_POSTGRES / "10-schema.sql").read_text())
            cur.execute((_POSTGRES / "20-seed.sql").read_text())
            cur.execute((_POSTGRES / "40-alerts-ui.sql").read_text())
            cur.execute((_POSTGRES / "50-operator-auth.sql").read_text())


@dataclass(frozen=True)
class Database:
    superuser_dsn: str
    gateway_dsn: str


@pytest.fixture(scope="module")
def database() -> Iterator[Database]:
    with PostgresContainer(_POSTGRES_IMAGE, driver=None) as postgres:
        superuser = _dsn(postgres, user=postgres.username, password=postgres.password)
        _apply(superuser)
        gateway = _dsn(
            postgres,
            user="svc_gateway",
            password="svc_gateway_dev",  # noqa: S106
        )
        yield Database(superuser, gateway)


async def test_login_with_seed_analyst(database: Database) -> None:
    settings = Settings(
        api_keys={"dev-scoring-key": "scoring"},
        database_url=database.gateway_dsn,
        jwt_secret=_TEST_JWT,
        metrics_port=0,
        features_url="http://features.test",
        detection_url="http://detection.test",
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            ok = await client.post(
                "/v1/auth/login",
                json={
                    "email": "analyst@featurestore.local",
                    "password": "demo-analyst-pass",
                },
            )
            bad = await client.post(
                "/v1/auth/login",
                json={
                    "email": "analyst@featurestore.local",
                    "password": "wrong",
                },
            )
            # Cookie Path=/api; until nginx, tests pass the cookie header explicitly.
            token = ok.cookies.get("fs_session")
            assert token
            me = await client.get(
                "/v1/auth/me",
                headers={"Cookie": f"fs_session={token}"},
            )
    assert ok.status_code == 204
    assert bad.status_code == 401
    assert me.status_code == 200
    body = me.json()
    assert body["email"] == "analyst@featurestore.local"
    assert body["role"] == "analyst"
    assert body["operator_id"]


async def test_gateway_cannot_read_other_ops_columns(database: Database) -> None:
    with psycopg.connect(database.gateway_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT operator_id, email, password_hash, role FROM ops.operators LIMIT 1")
            row = cur.fetchone()
            assert row is not None
            with pytest.raises(psycopg.Error):
                cur.execute("SELECT created_at FROM ops.operators LIMIT 1")
            with pytest.raises(psycopg.Error):
                cur.execute("SELECT audit_id FROM ops.audit_log LIMIT 1")
