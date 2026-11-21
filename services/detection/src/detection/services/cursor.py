"""Opaque keyset cursor for alert list pagination."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


class CursorError(ValueError):
    """Raised when a cursor string cannot be decoded."""


@dataclass(frozen=True, slots=True)
class AlertCursor:
    """Keyset position: ``(created_at, alert_id)``."""

    created_at: datetime
    alert_id: UUID


def encode_cursor(created_at: datetime, alert_id: UUID) -> str:
    """Encode a keyset position as an opaque base64url string."""
    payload = {
        "created_at": created_at.isoformat(),
        "alert_id": str(alert_id),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(value: str) -> AlertCursor:
    """Decode a cursor. Raises ``CursorError`` on any malformed input."""
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
        if not isinstance(payload, dict):
            raise CursorError("cursor must be an object")
        created_raw = payload["created_at"]
        alert_raw = payload["alert_id"]
        if not isinstance(created_raw, str) or not isinstance(alert_raw, str):
            raise CursorError("cursor fields must be strings")
        created_at = datetime.fromisoformat(created_raw)
        alert_id = UUID(alert_raw)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CursorError("invalid cursor") from exc
    return AlertCursor(created_at=created_at, alert_id=alert_id)
