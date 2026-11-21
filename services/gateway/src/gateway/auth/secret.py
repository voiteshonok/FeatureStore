"""Load the JWT signing key (ADR-020 / same pattern as ADR-009)."""

from __future__ import annotations

import os
import re
from pathlib import Path

DEFAULT_SECRET_PATH = Path("/run/secrets/jwt_secret")
_KEY_BYTES = 32
_HEX_KEY = re.compile(r"[0-9a-fA-F]+")


class JwtSecretError(Exception):
    """The JWT secret file is missing or is not a 32-byte hex key."""


def _resolve_path(path: str | Path | None) -> Path:
    if path is not None:
        return Path(path)
    configured = os.environ.get("JWT_SECRET_FILE")
    if configured:
        return Path(configured)
    return DEFAULT_SECRET_PATH


def load_jwt_secret(path: str | Path | None = None) -> bytes:
    """Read a 32-byte hex JWT secret.

    An explicit ``path`` wins. Otherwise ``JWT_SECRET_FILE`` is used, then
    ``/run/secrets/jwt_secret``.
    """
    key_path = _resolve_path(path)
    try:
        text = key_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        msg = f"JWT secret file not found: {key_path}"
        raise JwtSecretError(msg) from exc
    except UnicodeDecodeError as exc:
        msg = f"JWT secret file is not hexadecimal: {key_path}"
        raise JwtSecretError(msg) from exc
    stripped = text.strip()
    if _HEX_KEY.fullmatch(stripped) is None:
        msg = f"JWT secret file is not hexadecimal: {key_path}"
        raise JwtSecretError(msg)
    try:
        key = bytes.fromhex(stripped)
    except ValueError as exc:
        msg = f"JWT secret file is not hexadecimal: {key_path}"
        raise JwtSecretError(msg) from exc
    if len(key) != _KEY_BYTES:
        msg = f"JWT secret must be {_KEY_BYTES} bytes, got {len(key)}: {key_path}"
        raise JwtSecretError(msg)
    return key
