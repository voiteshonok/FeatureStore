"""Card and merchant windows stored in Redis.

Card windows are exact sorted-set ranges. Merchant windows span the last
1,440 UTC minute buckets (ADR-002, ADR-005) and are read via two-level
keys (ADR-016): hourly roll-ups plus minute hashes on incomplete edges.
Merchant sums run in one Lua ``EVAL`` (``merchant_read.lua``) so Python does
not unpack hundreds of HMGET replies per request.
"""

import importlib.resources
import inspect
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from schemas import FeatureSpec, for_entity
from schemas.catalog import Entity

_MERCHANT_READ_LUA = (
    importlib.resources.files("features").joinpath("merchant_read.lua").read_text(encoding="utf-8")
)
_MERCHANT_READ_SHA_ATTR = "_featurestore_merchant_read_sha"

MINUTE_MS = 60_000
DAY_MS = 24 * 60 * MINUTE_MS
MERCHANT_BUCKETS = 1_440
MERCHANT_TTL_S = 25 * 60 * 60
MINUTES_PER_HOUR = 60


def minute_bucket(event_time_ms: int) -> int:
    """UTC minute containing ``event_time_ms``.

    ClickHouse parity (ADR-005) uses the same bucket, once that SQL exists:
    ``intDiv(toUnixTimestamp64Milli(event_time), 60000)``. A 24 h merchant
    window is the 1,440 buckets ``(minute(T) - 1440, minute(T)]``.
    """
    return event_time_ms // MINUTE_MS


def hour_bucket(event_time_ms: int) -> int:
    """UTC hour containing ``event_time_ms`` (``minute_bucket // 60``)."""
    return minute_bucket(event_time_ms) // MINUTES_PER_HOUR


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
    """Legacy per-minute key (tests / migration). Prefer ``merchant_features_key``."""
    return f"mbkt:{merchant_id}:{bucket}"


def merchant_hour_key(merchant_id: str, hour: int) -> str:
    """Legacy per-hour key (tests / migration). Prefer ``merchant_features_key``."""
    return f"mhbk:{merchant_id}:{hour}"


def merchant_features_key(merchant_id: str) -> str:
    """Single hash: ``m:{minute}:c|d|s`` and ``h:{hour}:c|d|s`` (ADR-016)."""
    return f"mfeat:{merchant_id}"


def merchant_read_plan(first: int, current: int) -> tuple[list[int], list[int]]:
    """Split ``[first, current]`` into full hour indexes and edge minutes.

    Full hours are those whose 60 minutes lie entirely inside the window.
    Remaining minutes (incomplete left/right edges) are read individually.
    """
    if first > current:
        msg = f"merchant window empty: first={first} current={current}"
        raise ValueError(msg)
    hours: list[int] = []
    minutes: list[int] = []
    minute = first
    while minute <= current:
        hour = minute // MINUTES_PER_HOUR
        hour_start = hour * MINUTES_PER_HOUR
        hour_end = hour_start + MINUTES_PER_HOUR - 1
        if hour_start >= first and hour_end <= current:
            hours.append(hour)
            minute = hour_end + 1
        else:
            minutes.append(minute)
            minute += 1
    return hours, minutes


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


async def load_merchant_read_script(redis: Redis) -> str:
    """``SCRIPT LOAD`` merchant_read.lua and cache the SHA on the client."""
    sha = await _await(redis.script_load(_MERCHANT_READ_LUA))
    text = sha.decode() if isinstance(sha, bytes) else str(sha)
    setattr(redis, _MERCHANT_READ_SHA_ATTR, text)
    return text


async def merchant_stream_features(
    redis: Redis,
    merchant_id: str,
    t_ms: int,
) -> dict[str, int | float]:
    """Stream features of one merchant over the last 1,440 minute buckets.

    One ``mfeat:{id}`` hash holds hour and minute fields; ``merchant_read.lua``
    sums the ADR-016 plan in a single EVAL (ADR-016).
    """
    specs = stream_specs("merchant")
    current = minute_bucket(t_ms)
    first = current - MERCHANT_BUCKETS + 1
    total_count, total_declined = await _merchant_window_sum(
        redis,
        merchant_features_key(merchant_id),
        first,
        current,
    )
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


async def _merchant_window_sum(
    redis: Redis,
    features_key: str,
    first: int,
    current: int,
) -> tuple[int, int]:
    """Return ``(count, declined)`` for ``[first, current]`` via merchant_read.lua."""
    sha = getattr(redis, _MERCHANT_READ_SHA_ATTR, None)
    if not isinstance(sha, str):
        sha = await load_merchant_read_script(redis)
    args = (features_key, str(first), str(current))
    try:
        raw = await _await(redis.evalsha(sha, 1, *args))
    except NoScriptError:
        sha = await load_merchant_read_script(redis)
        raw = await _await(redis.evalsha(sha, 1, *args))
    return _parse_merchant_sum(raw)


def _parse_merchant_sum(raw: object) -> tuple[int, int]:
    if not isinstance(raw, (list, tuple)) or len(raw) != 2:
        msg = "merchant_read.lua returned an unexpected shape"
        raise TypeError(msg)
    return _counter(raw[0]), _counter(raw[1])


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
