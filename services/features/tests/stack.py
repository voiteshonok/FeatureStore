"""Kafka container for the online-writer integration tests."""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from datetime import timedelta

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from features.settings import Settings

KAFKA_IMAGE = "apache/kafka:3.9.0"
REDIS_IMAGE = "redis:8"
PARTITIONS = 4


def unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def kafka_container(host_port: int) -> DockerContainer:
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
    kafka.with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
    kafka.with_env("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
    kafka.waiting_for(
        LogMessageWaitStrategy("Kafka Server started").with_startup_timeout(timedelta(seconds=60))
    )
    return kafka


def ensure_topics(bootstrap: str) -> None:
    settings = Settings()
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap,
        client_id="features-tests",
        request_timeout_ms=5_000,
    )
    try:
        existing = set(admin.list_topics())
        topics = [
            NewTopic(name=name, num_partitions=PARTITIONS, replication_factor=1)
            for name in (settings.raw_topic, settings.updates_topic)
            if name not in existing
        ]
        if not topics:
            return
        try:
            admin.create_topics(topics, timeout_ms=10_000, wait_for_metadata=True)
        except TopicAlreadyExistsError:
            return
    finally:
        admin.close()


def wait_for_kafka(bootstrap: str, timeout_s: float = 60) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            ensure_topics(bootstrap)
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)
        else:
            return
    msg = f"Kafka did not become ready at {bootstrap}"
    raise TimeoutError(msg) from last_error


@dataclass
class KafkaService:
    container: DockerContainer
    bootstrap: str
