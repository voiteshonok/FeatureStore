"""HMAC tokenization of PANs and conversion to a downstream event."""

import hashlib
import hmac
import os
import re
from datetime import datetime
from pathlib import Path

from schemas import PaymentIn, TransactionEvent

DEFAULT_KEY_PATH = Path("/run/secrets/hmac_key")
_KEY_BYTES = 32
_HEX_KEY = re.compile(r"[0-9a-fA-F]+")


class HmacKeyError(Exception):
    """The HMAC key file is missing or is not a 32-byte hex key."""


def _resolve_key_path(path: str | Path | None) -> Path:
    if path is not None:
        return Path(path)
    configured = os.environ.get("HMAC_KEY_FILE")
    if configured:
        return Path(configured)
    return DEFAULT_KEY_PATH


def load_key(path: str | Path | None = None) -> bytes:
    """Read a 32-byte hex HMAC key.

    An explicit ``path`` wins. Otherwise ``HMAC_KEY_FILE`` is used, then
    ``/run/secrets/hmac_key``.
    """
    key_path = _resolve_key_path(path)
    try:
        text = key_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        msg = f"HMAC key file not found: {key_path}"
        raise HmacKeyError(msg) from exc
    except UnicodeDecodeError as exc:
        msg = f"HMAC key file is not hexadecimal: {key_path}"
        raise HmacKeyError(msg) from exc
    stripped = text.strip()
    if _HEX_KEY.fullmatch(stripped) is None:
        msg = f"HMAC key file is not hexadecimal: {key_path}"
        raise HmacKeyError(msg)
    try:
        key = bytes.fromhex(stripped)
    except ValueError as exc:
        msg = f"HMAC key file is not hexadecimal: {key_path}"
        raise HmacKeyError(msg) from exc
    if len(key) != _KEY_BYTES:
        msg = f"HMAC key must be {_KEY_BYTES} bytes, got {len(key)}: {key_path}"
        raise HmacKeyError(msg)
    return key


def tokenize(pan: str, key: bytes) -> str:
    """Return the lowercase hex HMAC-SHA-256 of ``pan``."""
    return hmac.digest(key, pan.encode(), hashlib.sha256).hex()


def to_event(payment: PaymentIn, key: bytes, now: datetime) -> TransactionEvent:
    """Copy ``payment`` without ``pan``, and set ``card_token`` and ``ingest_time``."""
    payload = payment.model_dump(exclude={"pan"})
    payload["card_token"] = tokenize(payment.pan, key)
    payload["ingest_time"] = now
    return TransactionEvent.model_validate(payload)
