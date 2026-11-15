"""Card and merchant windows stored in Redis.

Card windows are exact sorted-set ranges. Merchant windows are the last
1,440 UTC minute buckets (ADR-002, ADR-005). T-302 imports this module.
"""

import inspect
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from redis.asyncio import Redis

from schemas import FeatureSpec, for_entity
from schemas.catalog import Entity

MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS
MERCHANT_BUCKETS = 1_440
MERCHANT_TTL_S = 25 * 60 * 60


def minute_bucket(event_time_ms: int) -> int:
    """UTC minute containing ``event_time_ms``.

    ClickHouse parity (ADR-005) uses the same bucket, once that SQL exists:
    ``intDiv(toUnixTimestamp64Milli(event_time), 60000)``. A 24 h merchant
    window is the 1,440 buckets ``(minute(T) - 1440, minute(T)]``.
    """
    return event_time_ms // MINUTE_MS


def to_epoch_ms(value: datetime) -> int:
    """Milliseconds since the Unix epoch. ``value`` must be timezone-aware."""
    if value.tzinfo is None:
        msg = "datetime must be timezone-aware"
        raise ValueError(msg)
    utc = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = utc - epoch
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def card_key(card_token: str) -> str:
    return f"card:{card_token}"


def merchant_key(merchant_id: str, bucket: int) -> str:
    return f"mbkt:{merchant_id}:{bucket}"


def watermark_key(partition: int) -> str:
    return f"wm:{partition}"


def geo_key(card_token: str) -> str:
    return f"geo:{card_token}"


def member(event_id: UUID, amount_minor: int) -> str:
    return f"{event_id}|{amount_minor}"


def parse_member(value: str) -> tuple[str, int]:
    event_id, amount = value.split("|", 1)
    return event_id, int(amount)


def format_geo(lat: float, lon: float, event_time_ms: int) -> str:
    return f"{lat}|{lon}|{event_time_ms}"


def parse_geo(value: str) -> tuple[float, float, int]:
    lat_text, lon_text, time_text = value.split("|")
    return float(lat_text), float(lon_text), int(time_text)


def stream_specs(entity: Entity) -> tuple[FeatureSpec, ...]:
    return tuple(spec for spec in for_entity(entity) if spec.kind == "stream")


def window_ms(spec: FeatureSpec) -> int:
    if spec.window is None:
        msg = f"{spec.name} has no window"
        raise ValueError(msg)
    return int(spec.window.total_seconds() * 1000)


async def _await(value: object) -> Any:
    """Await a redis-py command.

    The shared command stubs return ``T | Awaitable[T]``. The asyncio client
    always returns a coroutine.
    """
    if not inspect.isawaitable(value):
        return value
    return await value


async def card_stream_features(
    redis: Redis,
    card_token: str,
    t_ms: int,
) -> dict[str, int | float]:
    """Stream features of one card at ``t_ms``, range ``(T - window, T]``."""
    specs = stream_specs("card")
    features: dict[str, int | float] = {spec.name: 0 for spec in specs}
    if not specs:
        return features
    widest = max(window_ms(spec) for spec in specs)
    raw = await _await(
        redis.zrangebyscore(
            card_key(card_token),
            f"({t_ms - widest}",
            t_ms,
            withscores=True,
        )
    )
    scored = _scored_members(raw)
    for spec in specs:
        start = t_ms - window_ms(spec)
        chosen = [item for item in scored if start < item[1] <= t_ms]
        if spec.agg == "count":
            features[spec.name] = len(chosen)
        elif spec.agg == "sum":
            features[spec.name] = sum(parse_member(item[0])[1] for item in chosen)
        else:
            msg = f"unsupported card aggregation {spec.agg}"
            raise ValueError(msg)
    return features


async def merchant_stream_features(
    redis: Redis,
    merchant_id: str,
    t_ms: int,
) -> dict[str, int | float]:
    """Stream features of one merchant over the last 1,440 minute buckets."""
    specs = stream_specs("merchant")
    current = minute_bucket(t_ms)
    first = current - MERCHANT_BUCKETS + 1
    pipe = redis.pipeline(transaction=False)
    for bucket in range(first, current + 1):
        pipe.hmget(merchant_key(merchant_id, bucket), "count", "declined")
    rows = await _await(pipe.execute())
    if not isinstance(rows, list):
        msg = "merchant window read returned an unexpected shape"
        raise TypeError(msg)
    total_count = 0
    total_declined = 0
    for row in rows:
        count, declined = _bucket_counts(row)
        total_count += count
        total_declined += declined
    features: dict[str, int | float] = {}
    for spec in specs:
        if spec.agg == "count":
            features[spec.name] = total_count
        elif spec.agg == "decline_rate":
            features[spec.name] = 0.0 if total_count == 0 else total_declined / total_count
        else:
            msg = f"unsupported merchant aggregation {spec.agg}"
            raise ValueError(msg)
    return features


def _scored_members(raw: object) -> list[tuple[str, float]]:
    if not isinstance(raw, list):
        msg = "card window read returned an unexpected shape"
        raise TypeError(msg)
    scored: list[tuple[str, float]] = []
    for item in raw:
        if not isinstance(item, tuple) or len(item) != 2:
            msg = "card window score pair was missing"
            raise TypeError(msg)
        member_value, score = item
        scored.append((str(member_value), float(score)))
    return scored


def _bucket_counts(row: object) -> tuple[int, int]:
    if not isinstance(row, (list, tuple)) or len(row) != 2:
        msg = "merchant bucket read returned an unexpected shape"
        raise TypeError(msg)
    return _counter(row[0]), _counter(row[1])


def _counter(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return int(value)
    if isinstance(value, str | int):
        return int(value)
    msg = "merchant bucket field was not a number"
    raise TypeError(msg)


async def read_offset(redis: Redis, partition: int) -> int | None:
    """Stored watermark offset, or ``None`` when the partition has no offset."""
    raw = await _await(redis.hget(watermark_key(partition), "offset"))
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return int(raw)
    return int(str(raw))
