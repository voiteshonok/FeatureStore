"""Redis + ClickHouse sources for pan_scan (US-1.2 / NFR-8)."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import pytest
import redis
from redis.asyncio import Redis as AsyncRedis
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.redis import RedisContainer

from features.offline import event_row, snapshot_row
from features.windows import card_key, merchant_features_key, to_epoch_ms
from features.worker import apply_event, load_script
from pan_scan.scan import (
    ClickHouseSource,
    RedisSource,
    mask_pan,
    scan_records,
)
from schemas import FeatureSnapshot, TransactionEvent

pytestmark = pytest.mark.integration

PAN = "4111111111111111"
MASKED = mask_pan(PAN)
REDIS_IMAGE = "redis:8.0-alpine"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.3"
_REPO_ROOT = Path(__file__).resolve().parents[3]
CLICKHOUSE_INIT_DIR = _REPO_ROOT / "infra" / "clickhouse"
_SVC_OFFLINE_PASSWORD = "svc_offline_dev"  # noqa: S105
_BASE = datetime.now(UTC).replace(microsecond=0)


def _make_event(**overrides: object) -> TransactionEvent:
    payload: dict[str, object] = {
        "event_id": uuid4(),
        "payment_id": "pay-clean",
        "merchant_id": "m-clean",
        "amount_minor": 100,
        "currency": "EUR",
        "status": "approved",
        "event_time": _BASE,
        "card_token": "card-clean-token",
        "ingest_time": _BASE,
        "lat": 52.5,
        "lon": 13.4,
    }
    payload.update(overrides)
    return TransactionEvent.model_validate(payload)


@pytest.fixture
def redis_url() -> str:
    with RedisContainer(REDIS_IMAGE) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
def clickhouse_service():
    clickhouse = (
        ClickHouseContainer(
            CLICKHOUSE_IMAGE,
            username="featurestore",
            password="featurestore",  # noqa: S106
            dbname="featurestore",
        )
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_env("SVC_OFFLINE_PASSWORD", _SVC_OFFLINE_PASSWORD)
        .with_env("SVC_FEATURES_READ_PASSWORD", "svc_features_read_dev")
        .with_volume_mapping(str(CLICKHOUSE_INIT_DIR), "/docker-entrypoint-initdb.d", "ro")
    )
    with clickhouse:
        yield clickhouse


def _ch_env(clickhouse: ClickHouseContainer) -> None:
    host = clickhouse.get_container_host_ip()
    port = int(clickhouse.get_exposed_port(8123))
    os.environ["CLICKHOUSE_URL"] = f"http://{host}:{port}"
    os.environ["CLICKHOUSE_USER"] = "svc_offline"
    os.environ["CLICKHOUSE_PASSWORD"] = _SVC_OFFLINE_PASSWORD
    os.environ["CLICKHOUSE_DB"] = "featurestore"


def _offline_client(clickhouse: ClickHouseContainer):
    return clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),
        port=int(clickhouse.get_exposed_port(8123)),
        username="svc_offline",
        password=_SVC_OFFLINE_PASSWORD,
        database="featurestore",
    )


def test_planted_pans_are_found_and_masked(
    redis_url: str, clickhouse_service: ClickHouseContainer
) -> None:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    try:
        client.zadd(card_key("plant-card"), {f"{uuid4()}|{PAN}": 1.0})
        client.hset(merchant_features_key("plant-m"), mapping={f"m:{PAN}:c": "1"})
    finally:
        client.close()

    offline = _offline_client(clickhouse_service)
    try:
        planted = _make_event(payment_id=PAN, card_token="plant-ch")  # noqa: S106
        offline.insert(
            "events",
            [
                event_row(
                    planted,
                    partition=0,
                    offset=0,
                )
            ],
            column_names=[
                "schema_version",
                "event_id",
                "payment_id",
                "merchant_id",
                "amount_minor",
                "currency",
                "status",
                "event_time",
                "card_token",
                "lat",
                "lon",
                "ingest_time",
                "kafka_partition",
                "kafka_offset",
            ],
        )
    finally:
        offline.close()

    os.environ["REDIS_URL"] = redis_url
    _ch_env(clickhouse_service)

    redis_findings = scan_records("redis", list(RedisSource().records()))
    ch_findings = scan_records("clickhouse", list(ClickHouseSource().records()))
    findings = redis_findings + ch_findings

    assert {item.masked for item in findings} == {MASKED}
    assert all(PAN not in item.masked for item in findings)
    assert any(item.source == "redis" for item in findings)
    assert any(item.source == "clickhouse" for item in findings)
    assert len(findings) >= 3


@pytest.mark.asyncio
async def test_clean_apply_and_offline_rows_find_nothing(
    redis_url: str, clickhouse_service: ClickHouseContainer
) -> None:
    event = _make_event()
    async_redis = AsyncRedis.from_url(redis_url, decode_responses=True)
    try:
        sha = await load_script(async_redis)
        outcome = await apply_event(
            async_redis,
            sha,
            event,
            partition=0,
            offset=1,
            now_ms=to_epoch_ms(event.ingest_time) + 1_000,
        )
        assert outcome == "applied"
    finally:
        await async_redis.aclose()

    snapshot = FeatureSnapshot(
        entity_type="card",
        entity_id=event.card_token,
        event_id=event.event_id,
        event_time=event.event_time,
        ingest_time=event.ingest_time,
        features={"card_cnt_1m": 1.0},
        source_partition=0,
        source_offset=1,
    )
    offline = _offline_client(clickhouse_service)
    try:
        offline.insert(
            "events",
            [event_row(event, partition=0, offset=1)],
            column_names=[
                "schema_version",
                "event_id",
                "payment_id",
                "merchant_id",
                "amount_minor",
                "currency",
                "status",
                "event_time",
                "card_token",
                "lat",
                "lon",
                "ingest_time",
                "kafka_partition",
                "kafka_offset",
            ],
        )
        offline.insert(
            "feature_snapshots",
            [snapshot_row(snapshot)],
            column_names=[
                "entity_type",
                "entity_id",
                "event_id",
                "event_time",
                "ingest_time",
                "features",
            ],
        )
    finally:
        offline.close()

    os.environ["REDIS_URL"] = redis_url
    _ch_env(clickhouse_service)

    redis_records = list(RedisSource().records())
    ch_records = list(ClickHouseSource().records())
    assert redis_records
    assert ch_records
    assert scan_records("redis", redis_records) == []
    assert scan_records("clickhouse", ch_records) == []
