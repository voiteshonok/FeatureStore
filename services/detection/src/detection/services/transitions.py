"""Alert status transitions and operator role checks."""

from typing import Literal

OperatorRole = Literal["viewer", "analyst", "admin"]
Status = Literal["open", "acknowledged", "resolved"]

_ALLOWED: frozenset[tuple[str, str]] = frozenset(
    {
        ("open", "acknowledged"),
        ("acknowledged", "resolved"),
        ("open", "resolved"),
    }
)
_MUTATORS: frozenset[str] = frozenset({"analyst", "admin"})


def transition_allowed(current: str, new: str) -> bool:
    """Return whether ``current`` may move to ``new``.

    Allowed paths are open→acknowledged, acknowledged→resolved, and
    open→resolved.
    """
    return (current, new) in _ALLOWED


def can_mutate(role: str) -> bool:
    """Return whether the operator role may change alert status.

    ``viewer`` is read-only; ``analyst`` and ``admin`` may transition.
    """
    return role in _MUTATORS
