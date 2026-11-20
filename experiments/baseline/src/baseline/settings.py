"""Process settings for the SQL measurement stand."""

import os
from dataclasses import dataclass
from typing import Self

_DEFAULT_BOOTSTRAP = "localhost:29092"
_DEFAULT_DATABASE_URL = "postgresql://svc_baseline:svc_baseline_dev@localhost:5432/featurestore"


@dataclass(frozen=True)
class Settings:
    """Kafka, Postgres, batch, and pool settings.

    The consumer group id is ``baseline-writer`` (ADR-015). ``metrics_port``
    serves Prometheus; 0 leaves the server off (ADR-014).
    """

    kafka_bootstrap: str = _DEFAULT_BOOTSTRAP
    database_url: str = _DEFAULT_DATABASE_URL
    group_id: str = "baseline-writer"
    raw_topic: str = "transactions.raw"
    batch_max_records: int = 200
    batch_wait_ms: int = 50
    connect_backoff_s: float = 0.5
    connect_backoff_max_s: float = 5.0
    metrics_port: int = 9000
    pool_min_size: int = 4
    pool_max_size: int = 16

    def __post_init__(self) -> None:
        if self.batch_max_records < 1:
            msg = "batch_max_records must be positive"
            raise ValueError(msg)
        if self.batch_wait_ms < 1:
            msg = "batch_wait_ms must be positive"
            raise ValueError(msg)
        if self.pool_min_size < 1 or self.pool_max_size < self.pool_min_size:
            msg = "pool size must be positive and max must cover min"
            raise ValueError(msg)
        if self.connect_backoff_s <= 0 or self.connect_backoff_max_s < self.connect_backoff_s:
            msg = "connect backoff must be positive and max must cover the initial delay"
            raise ValueError(msg)
        if not 0 <= self.metrics_port <= 65535:
            msg = "metrics_port must be in 0..65535"
            raise ValueError(msg)

    @classmethod
    def from_env(cls) -> Self:
        """Read settings from the environment."""
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", _DEFAULT_BOOTSTRAP),
            database_url=os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL),
            batch_max_records=int(os.environ.get("BASELINE_BATCH_MAX_RECORDS", "200")),
            batch_wait_ms=int(os.environ.get("BASELINE_BATCH_WAIT_MS", "50")),
            connect_backoff_s=float(os.environ.get("BASELINE_CONNECT_BACKOFF_S", "0.5")),
            connect_backoff_max_s=float(os.environ.get("BASELINE_CONNECT_BACKOFF_MAX_S", "5")),
            metrics_port=int(os.environ.get("METRICS_PORT", "9000")),
            pool_min_size=int(os.environ.get("BASELINE_POOL_MIN_SIZE", "4")),
            pool_max_size=int(os.environ.get("BASELINE_POOL_MAX_SIZE", "16")),
        )
