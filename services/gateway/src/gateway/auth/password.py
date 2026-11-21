"""bcrypt password checks (ADR-020)."""

from __future__ import annotations

import bcrypt

# Fixed dummy hash so missing-email logins still pay for checkpw (timing).
_DUMMY_HASH = bcrypt.hashpw(b"timing-dummy-password", bcrypt.gensalt(rounds=4))


def passwords_match(password: str, password_hash: str | None) -> bool:
    """Return whether ``password`` matches ``password_hash``.

    When ``password_hash`` is missing, still runs ``checkpw`` against a dummy
    hash so response timing does not reveal whether the email exists.
    """
    candidate = password.encode("utf-8")
    if password_hash is None:
        bcrypt.checkpw(candidate, _DUMMY_HASH)
        return False
    try:
        stored = password_hash.encode("utf-8")
        return bcrypt.checkpw(candidate, stored)
    except (ValueError, TypeError):
        bcrypt.checkpw(candidate, _DUMMY_HASH)
        return False
