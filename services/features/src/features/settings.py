"""Process settings for the online writer."""

import os
from dataclasses import dataclass
from typing import Self

_DEFAULT_BOOTSTRAP = "localhost:29092"
_DEFAULT_REDIS_URL = "redis://localhost:6379/0"


@dataclass(frozen=True)
class Settings:
    """Kafka, Redis, and heartbeat settings.

    The consumer group id is ``online-writer`` (ADR-001). A healthy idle
    partition must be refreshed inside the 5 s card ``stale_after``, so the
    heartbeat interval cannot exceed 2 s (ADR-013).
    """

    kafka_bootstrap: str = _DEFAULT_BOOTSTRAP
    redis_url: str = _DEFAULT_REDIS_URL
    group_id: str = "online-writer"
    raw_topic: str = "transactions.raw"
    updates_topic: str = "features.updates"
    heartbeat_interval_s: float = 1.0
    redis_backoff_s: float = 0.5
    redis_backoff_max_s: float = 5.0

    def __post_init__(self) -> None:
        if not 0 < self.heartbeat_interval_s <= 2:
            msg = "heartbeat_interval_s must be in the interval (0, 2]"
            raise ValueError(msg)
        if self.redis_backoff_s <= 0 or self.redis_backoff_max_s < self.redis_backoff_s:
            msg = "redis backoff must be positive and max must cover the initial delay"
            raise ValueError(msg)

    @classmethod
    def from_env(cls) -> Self:
        """Read settings from the environment."""
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", _DEFAULT_BOOTSTRAP),
            redis_url=os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL),
            heartbeat_interval_s=float(os.environ.get("HEARTBEAT_INTERVAL_S", "1")),
            redis_backoff_s=float(os.environ.get("REDIS_BACKOFF_S", "0.5")),
            redis_backoff_max_s=float(os.environ.get("REDIS_BACKOFF_MAX_S", "5")),
        )
