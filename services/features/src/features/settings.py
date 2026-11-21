"""Process settings for the online writer, offline writer, and the feature API."""

import os
from dataclasses import dataclass
from typing import Self

_DEFAULT_BOOTSTRAP = "localhost:29092"
_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_DEFAULT_DATABASE_URL = "postgresql://svc_features:svc_features_dev@localhost:5432/featurestore"
_DEFAULT_CLICKHOUSE_URL = "http://localhost:8123"
_DEFAULT_CLICKHOUSE_USER = "svc_offline"
_DEFAULT_CLICKHOUSE_PASSWORD = "svc_offline_dev"  # noqa: S105
_DEFAULT_CLICKHOUSE_DB = "featurestore"


@dataclass(frozen=True)
class Settings:
    """Kafka, Redis, Postgres, ClickHouse, and heartbeat settings.

    The online consumer group id is ``online-writer`` (ADR-001). A healthy idle
    partition must be refreshed inside the 5 s card ``stale_after``, so the
    heartbeat interval cannot exceed 2 s (ADR-013). ``n_partitions`` is the
    partition count of ``transactions.raw`` the API checks at startup.
    ``metrics_port`` serves Prometheus; 0 leaves the server off (ADR-014).
    Offline-writer fields (ADR-018) are ignored by the online writer and API.
    """

    kafka_bootstrap: str = _DEFAULT_BOOTSTRAP
    redis_url: str = _DEFAULT_REDIS_URL
    database_url: str = _DEFAULT_DATABASE_URL
    group_id: str = "online-writer"
    raw_topic: str = "transactions.raw"
    updates_topic: str = "features.updates"
    n_partitions: int = 4
    heartbeat_interval_s: float = 1.0
    redis_backoff_s: float = 0.5
    redis_backoff_max_s: float = 5.0
    metrics_port: int = 9000
    clickhouse_url: str = _DEFAULT_CLICKHOUSE_URL
    clickhouse_user: str = _DEFAULT_CLICKHOUSE_USER
    clickhouse_password: str = _DEFAULT_CLICKHOUSE_PASSWORD
    clickhouse_db: str = _DEFAULT_CLICKHOUSE_DB
    offline_batch_rows: int = 5000
    offline_flush_interval_s: float = 1.0
    offline_backoff_s: float = 0.5
    offline_backoff_max_s: float = 10.0

    def __post_init__(self) -> None:
        if self.n_partitions < 1:
            msg = "n_partitions must be positive"
            raise ValueError(msg)
        if not 0 < self.heartbeat_interval_s <= 2:
            msg = "heartbeat_interval_s must be in the interval (0, 2]"
            raise ValueError(msg)
        if self.redis_backoff_s <= 0 or self.redis_backoff_max_s < self.redis_backoff_s:
            msg = "redis backoff must be positive and max must cover the initial delay"
            raise ValueError(msg)
        if not 0 <= self.metrics_port <= 65535:
            msg = "metrics_port must be in 0..65535"
            raise ValueError(msg)
        if self.offline_batch_rows < 1:
            msg = "offline_batch_rows must be positive"
            raise ValueError(msg)
        if self.offline_flush_interval_s <= 0:
            msg = "offline_flush_interval_s must be positive"
            raise ValueError(msg)
        if self.offline_backoff_s <= 0 or self.offline_backoff_max_s < self.offline_backoff_s:
            msg = "offline backoff must be positive and max must cover the initial delay"
            raise ValueError(msg)

    @classmethod
    def from_env(cls) -> Self:
        """Read settings from the environment."""
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", _DEFAULT_BOOTSTRAP),
            redis_url=os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL),
            database_url=os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL),
            n_partitions=int(os.environ.get("N_PARTITIONS", "4")),
            heartbeat_interval_s=float(os.environ.get("HEARTBEAT_INTERVAL_S", "1")),
            redis_backoff_s=float(os.environ.get("REDIS_BACKOFF_S", "0.5")),
            redis_backoff_max_s=float(os.environ.get("REDIS_BACKOFF_MAX_S", "5")),
            metrics_port=int(os.environ.get("METRICS_PORT", "9000")),
            clickhouse_url=os.environ.get("CLICKHOUSE_URL", _DEFAULT_CLICKHOUSE_URL),
            clickhouse_user=os.environ.get("CLICKHOUSE_USER", _DEFAULT_CLICKHOUSE_USER),
            clickhouse_password=os.environ.get(
                "CLICKHOUSE_PASSWORD",
                os.environ.get("SVC_OFFLINE_PASSWORD", _DEFAULT_CLICKHOUSE_PASSWORD),
            ),
            clickhouse_db=os.environ.get("CLICKHOUSE_DB", _DEFAULT_CLICKHOUSE_DB),
            offline_batch_rows=int(os.environ.get("OFFLINE_BATCH_ROWS", "5000")),
            offline_flush_interval_s=float(os.environ.get("OFFLINE_FLUSH_INTERVAL_S", "1")),
            offline_backoff_s=float(os.environ.get("OFFLINE_BACKOFF_S", "0.5")),
            offline_backoff_max_s=float(os.environ.get("OFFLINE_BACKOFF_MAX_S", "10")),
        )
