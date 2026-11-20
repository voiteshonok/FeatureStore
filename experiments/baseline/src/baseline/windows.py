"""Card and merchant window bounds for the SQL stand.

Must match ``services/features/src/features/windows.py``. This package does not
import that module (ADR-015).
"""

from datetime import UTC, datetime, timedelta

MINUTE_MS = 60_000
MERCHANT_BUCKETS = 1_440


def to_epoch_ms(value: datetime) -> int:
    """Milliseconds since the Unix epoch. ``value`` must be timezone-aware."""
    if value.tzinfo is None:
        msg = "datetime must be timezone-aware"
        raise ValueError(msg)
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def from_epoch_ms(epoch_ms: int) -> datetime:
    """UTC datetime at millisecond precision."""
    seconds, millis = divmod(epoch_ms, 1000)
    return datetime.fromtimestamp(seconds, UTC).replace(microsecond=millis * 1000)


def card_bounds(t: datetime, window: timedelta) -> tuple[datetime, datetime]:
    """Exclusive start and inclusive end of one card window: ``(T - window, T]``."""
    return t - window, t


def in_card_window(event_time: datetime, t: datetime, window: timedelta) -> bool:
    """True when ``event_time`` falls inside ``(T - window, T]``."""
    start, end = card_bounds(t, window)
    return start < event_time <= end


def merchant_bounds(t: datetime) -> tuple[datetime, datetime]:
    """Inclusive start and exclusive end of the last 1,440 UTC minute buckets.

    The current minute is included. The bucket ``minute(T) - 1440`` is not.
    """
    current = to_epoch_ms(t) // MINUTE_MS
    first = current - MERCHANT_BUCKETS + 1
    return from_epoch_ms(first * MINUTE_MS), from_epoch_ms((current + 1) * MINUTE_MS)


def in_merchant_window(event_time: datetime, t: datetime) -> bool:
    """True when ``event_time`` falls in the merchant's 1,440 minute buckets."""
    start, end = merchant_bounds(t)
    return start <= event_time < end
