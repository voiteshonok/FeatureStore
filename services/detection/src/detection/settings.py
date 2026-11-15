"""Process settings for the detection consumer and alert API."""

import os
from dataclasses import dataclass
from typing import Self

_DEFAULT_BOOTSTRAP = "localhost:29092"
_DEFAULT_DATABASE_URL = "postgresql://svc_detection:svc_detection_dev@localhost:5432/featurestore"


@dataclass(frozen=True)
class Settings:
    """Kafka and Postgres settings.

    The consumer group id is ``detection`` (ADR-001). Offsets are committed
    only after the alert write. ``rule_reload_s`` is how often enabled rules
    are read again.
    """

    kafka_bootstrap: str = _DEFAULT_BOOTSTRAP
    database_url: str = _DEFAULT_DATABASE_URL
    group_id: str = "detection"
    updates_topic: str = "features.updates"
    rule_reload_s: float = 60.0
    backoff_s: float = 0.5
    backoff_max_s: float = 5.0

    def __post_init__(self) -> None:
        if self.rule_reload_s <= 0:
            msg = "rule_reload_s must be positive"
            raise ValueError(msg)
        if self.backoff_s <= 0 or self.backoff_max_s < self.backoff_s:
            msg = "backoff must be positive and max must cover the initial delay"
            raise ValueError(msg)

    @classmethod
    def from_env(cls) -> Self:
        """Read settings from the environment."""
        return cls(
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", _DEFAULT_BOOTSTRAP),
            database_url=os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL),
            rule_reload_s=float(os.environ.get("RULE_RELOAD_S", "60")),
            backoff_s=float(os.environ.get("BACKOFF_S", "0.5")),
            backoff_max_s=float(os.environ.get("BACKOFF_MAX_S", "5")),
        )
