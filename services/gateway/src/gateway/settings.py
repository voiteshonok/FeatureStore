"""Process settings for the API gateway."""

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Self

from limits import parse_many

_DEFAULT_FEATURES_URL = "http://localhost:8001"
_DEFAULT_DETECTION_URL = "http://localhost:8002"
_DEFAULT_DATABASE_URL = "postgresql://svc_gateway:featurestore@localhost:5432/featurestore"
_DEFAULT_RATE_LIMIT = "100/second"


def parse_api_keys(raw: str) -> dict[str, str]:
    """Parse ``client:key`` pairs into a map of key to client name.

    Entries are comma-separated. The split is the first colon, so a key may
    contain colons. Empty input yields an empty map.
    """
    mapping: dict[str, str] = {}
    clients: set[str] = set()
    if not raw.strip():
        return mapping
    for item in raw.split(","):
        piece = item.strip()
        if not piece:
            continue
        client, separator, key = piece.partition(":")
        client = client.strip()
        key = key.strip()
        if separator != ":" or not client or not key:
            msg = "api key entry must be client:key"
            raise ValueError(msg)
        if client in clients or key in mapping:
            msg = "api key entries must be unique"
            raise ValueError(msg)
        clients.add(client)
        mapping[key] = client
    return mapping


@dataclass(frozen=True)
class Settings:
    """Gateway settings.

    ``api_keys`` maps the secret to the client name used in logs and the
    rate-limit bucket. ``rate_limit`` is a limits string, default ``100/second``
    (ADR-008).
    """

    api_keys: Mapping[str, str] = field(default_factory=dict)
    rate_limit: str = _DEFAULT_RATE_LIMIT
    features_url: str = _DEFAULT_FEATURES_URL
    detection_url: str = _DEFAULT_DETECTION_URL
    database_url: str = _DEFAULT_DATABASE_URL

    def __post_init__(self) -> None:
        parsed = parse_many(self.rate_limit)
        if not parsed:
            msg = "rate limit is empty"
            raise ValueError(msg)
        keys = dict(self.api_keys)
        for key, client in keys.items():
            if not key or not client:
                msg = "api key entries must be client:key"
                raise ValueError(msg)
        object.__setattr__(self, "api_keys", MappingProxyType(keys))

    @classmethod
    def from_env(cls) -> Self:
        """Read settings from the environment."""
        return cls(
            api_keys=parse_api_keys(os.environ.get("GATEWAY_API_KEYS", "")),
            rate_limit=os.environ.get("GATEWAY_RATE_LIMIT", _DEFAULT_RATE_LIMIT),
            features_url=os.environ.get("FEATURES_URL", _DEFAULT_FEATURES_URL),
            detection_url=os.environ.get("DETECTION_URL", _DEFAULT_DETECTION_URL),
            database_url=os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL),
        )
