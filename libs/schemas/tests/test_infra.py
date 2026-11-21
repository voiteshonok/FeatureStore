from __future__ import annotations

import socket
from datetime import timedelta
from pathlib import Path

import clickhouse_connect
import psycopg
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from kafka import KafkaConsumer, KafkaProducer
from testcontainers.community.clickhouse import ClickHouseContainer
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

KAFKA_IMAGE = "apache/kafka:3.9.0"
REDIS_IMAGE = "redis:8"
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:25.3"
POSTGRES_IMAGE = "postgres:17"
PROBE_VALUE = "probe-value"

_REPO_ROOT = Path(__file__).resolve().parents[3]
CLICKHOUSE_INIT_DIR = _REPO_ROOT / "infra" / "clickhouse"
_SVC_OFFLINE_PASSWORD = "svc_offline_dev"  # noqa: S105
_SVC_FEATURES_READ_PASSWORD = "svc_features_read_dev"  # noqa: S105
_EVENT_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_INSERT_EVENT = """
INSERT INTO featurestore.events (
    schema_version, event_id, payment_id, merchant_id, amount_minor, currency,
    status, event_time, card_token, lat, lon, ingest_time, kafka_partition, kafka_offset
) VALUES (
    1, 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'pay-1', 'm1', 100, 'USD',
    'approved', now64(3), 'tok-1', NULL, NULL, now64(3), 0, 1
)
"""
_INSERT_SNAPSHOT = """
INSERT INTO featurestore.feature_snapshots (
    entity_type, entity_id, event_id, event_time, ingest_time, features
) VALUES (
    'card', 'tok-1', 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee',
    now64(3), now64(3), map('tx_count_1h', 1.0)
)
"""


def _unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _ch_client(clickhouse: ClickHouseContainer, *, username: str, password: str):
    return clickhouse_connect.get_client(
        host=clickhouse.get_container_host_ip(),
        port=int(clickhouse.get_exposed_port(8123)),
        username=username,
        password=password,
        database="featurestore",
    )


def _roundtrip_kafka() -> None:
    host_port = _unused_port()
    kafka = DockerContainer(KAFKA_IMAGE)
    kafka.with_bind_ports(9092, host_port)
    kafka.with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
    kafka.with_env("KAFKA_NODE_ID", "1")
    kafka.with_env("KAFKA_PROCESS_ROLES", "broker,controller")
    kafka.with_env("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
    kafka.with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{host_port}")
    kafka.with_env(
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
        "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
    )
    kafka.with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
    kafka.with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
    kafka.with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
    kafka.with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    kafka.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    kafka.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    kafka.with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    kafka.with_env("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
    kafka.waiting_for(
        LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(timedelta(seconds=60))
    )
    bootstrap = f"127.0.0.1:{host_port}"
    with kafka:
        producer = KafkaProducer(bootstrap_servers=bootstrap)
        producer.send("probe", PROBE_VALUE.encode()).get(timeout=10)
        producer.flush()
        producer.close()
        consumer = KafkaConsumer(
            "probe",
            bootstrap_servers=bootstrap,
            auto_offset_reset="earliest",
            consumer_timeout_ms=15_000,
            group_id="probe-group",
        )
        try:
            records = list(consumer)
        finally:
            consumer.close()
        assert records[0].value == PROBE_VALUE.encode()


def _roundtrip_redis() -> None:
    with RedisContainer(REDIS_IMAGE) as redis_container:
        client = redis_container.get_client()
        client.set("probe", PROBE_VALUE)
        assert client.get("probe") == PROBE_VALUE.encode()


def _roundtrip_clickhouse() -> None:
    clickhouse = (
        ClickHouseContainer(
            CLICKHOUSE_IMAGE,
            username="featurestore",
            password="featurestore",  # noqa: S106
            dbname="featurestore",
        )
        .with_env("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_env("SVC_OFFLINE_PASSWORD", _SVC_OFFLINE_PASSWORD)
        .with_env("SVC_FEATURES_READ_PASSWORD", _SVC_FEATURES_READ_PASSWORD)
        .with_volume_mapping(str(CLICKHOUSE_INIT_DIR), "/docker-entrypoint-initdb.d", "ro")
    )
    with clickhouse:
        offline = _ch_client(clickhouse, username="svc_offline", password=_SVC_OFFLINE_PASSWORD)
        features_read = _ch_client(
            clickhouse, username="svc_features_read", password=_SVC_FEATURES_READ_PASSWORD
        )
        try:
            offline.command(_INSERT_EVENT)
            offline.command(_INSERT_SNAPSHOT)
            events = offline.query(
                "SELECT event_id FROM featurestore.events WHERE event_id = {eid:UUID}",
                parameters={"eid": _EVENT_ID},
            ).result_rows
            assert len(events) == 1

            with pytest.raises(DatabaseError):
                offline.command("DROP TABLE featurestore.events")
            with pytest.raises(DatabaseError):
                offline.command("CREATE TABLE featurestore.probe (x String) ENGINE = Memory")

            with pytest.raises(DatabaseError):
                features_read.command(_INSERT_SNAPSHOT)
            with pytest.raises(DatabaseError):
                features_read.query("SELECT count() FROM featurestore.events")

            snapshots = features_read.query(
                "SELECT entity_id FROM featurestore.feature_snapshots WHERE event_id = {eid:UUID}",
                parameters={"eid": _EVENT_ID},
            ).result_rows
            assert snapshots == [("tok-1",)]
        finally:
            offline.close()
            features_read.close()


def _roundtrip_postgres() -> None:
    with PostgresContainer(POSTGRES_IMAGE, driver=None) as postgres:
        with psycopg.connect(
            host=postgres.get_container_host_ip(),
            port=postgres.get_exposed_port(postgres.port),
            user=postgres.username,
            password=postgres.password,
            dbname=postgres.dbname,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE probe (x text)")
                cur.execute("INSERT INTO probe VALUES (%s)", (PROBE_VALUE,))
                cur.execute("SELECT x FROM probe")
                assert cur.fetchone() == (PROBE_VALUE,)
            conn.commit()


@pytest.mark.integration
def test_infra_roundtrip() -> None:
    _roundtrip_kafka()
    _roundtrip_redis()
    _roundtrip_clickhouse()
    _roundtrip_postgres()
