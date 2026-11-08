from datetime import UTC, datetime, timedelta
from typing import Annotated

from pydantic import AfterValidator, AwareDatetime

_MAX_FUTURE = timedelta(seconds=5)
_MAX_AGE = timedelta(hours=24)


def to_utc_milliseconds(value: datetime) -> datetime:
    """Reject naive datetimes; convert to UTC and truncate to milliseconds."""
    if value.tzinfo is None:
        msg = "datetime must be timezone-aware"
        raise ValueError(msg)
    utc = value.astimezone(UTC)
    return utc.replace(microsecond=(utc.microsecond // 1000) * 1000)


UtcMsDateTime = Annotated[AwareDatetime, AfterValidator(to_utc_milliseconds)]


def check_event_time(event_time: datetime, now: datetime) -> str | None:
    """Return an error reason if ``event_time`` is too far from ``now``.

    Accepted window: ``[now - 24h, now + 5s]``. Both endpoints are inclusive.
    """
    event_utc = to_utc_milliseconds(event_time)
    now_utc = to_utc_milliseconds(now)
    if event_utc > now_utc + _MAX_FUTURE:
        return "event_time is more than 5 seconds ahead of now"
    if event_utc < now_utc - _MAX_AGE:
        return "event_time is older than 24 hours"
    return None
