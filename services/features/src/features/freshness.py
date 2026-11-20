"""ADR-013 freshness for one card and merchant at a caller-supplied now."""

import asyncio
from datetime import UTC, datetime

from redis.asyncio import Redis

from features.geo import velocity_kmh
from features.windows import (
    _await,
    card_stream_features,
    geo_key,
    merchant_stream_features,
    parse_geo,
    stream_specs,
    to_epoch_ms,
    watermark_key,
)
from schemas import CATALOG, FeatureRequest, FeatureSpec, FeatureStatus, FeatureValue, partition_for


def from_epoch_ms(epoch_ms: int) -> datetime:
    """UTC datetime at millisecond precision."""
    seconds, millis = divmod(epoch_ms, 1000)
    return datetime.fromtimestamp(seconds, UTC).replace(microsecond=millis * 1000)


def _default(spec: FeatureSpec) -> FeatureValue:
    return FeatureValue(value=spec.default, status=FeatureStatus.default)


def _measured(
    spec: FeatureSpec,
    value: int | float,
    as_of_ms: int,
    now_ms: int,
) -> FeatureValue:
    if spec.stale_after is None:
        msg = f"{spec.name} has no stale_after"
        raise ValueError(msg)
    age = max(0, now_ms - as_of_ms)
    limit = int(spec.stale_after.total_seconds() * 1000)
    status = FeatureStatus.fresh if age <= limit else FeatureStatus.stale
    return FeatureValue(
        value=value,
        as_of=from_epoch_ms(as_of_ms),
        age_ms=age,
        status=status,
    )


async def _watermarks(redis: Redis, n_partitions: int) -> list[int | None]:
    pipe = redis.pipeline(transaction=False)
    for partition in range(n_partitions):
        pipe.hget(watermark_key(partition), "as_of_ms")
    rows = await _await(pipe.execute())
    if not isinstance(rows, list) or len(rows) != n_partitions:
        msg = "watermark read returned an unexpected shape"
        raise TypeError(msg)
    parsed: list[int | None] = []
    for row in rows:
        parsed.append(None if row is None else int(row))
    return parsed


def _card_from_values(
    values: dict[str, int | float] | None,
    *,
    as_of_ms: int | None,
    now_ms: int,
) -> dict[str, FeatureValue]:
    specs = stream_specs("card")
    if as_of_ms is None or values is None:
        return {spec.name: _default(spec) for spec in specs}
    if all(values[spec.name] == 0 for spec in specs):
        # Empty zset yields zeros; treat as default (no as_of).
        return {spec.name: _default(spec) for spec in specs}
    return {spec.name: _measured(spec, values[spec.name], as_of_ms, now_ms) for spec in specs}


def _merchant_from_values(
    values: dict[str, int | float],
    *,
    watermarks: list[int | None],
    now_ms: int,
) -> dict[str, FeatureValue]:
    specs = stream_specs("merchant")
    present = [value for value in watermarks if value is not None]
    if len(present) != len(watermarks):
        return {spec.name: _default(spec) for spec in specs}
    count_name = next(spec.name for spec in specs if spec.agg == "count")
    if values[count_name] == 0:
        return {spec.name: _default(spec) for spec in specs}
    as_of_ms = min(present)
    return {spec.name: _measured(spec, values[spec.name], as_of_ms, now_ms) for spec in specs}


def _geo_from_raw(
    raw: object,
    request: FeatureRequest,
    spec: FeatureSpec,
) -> FeatureValue:
    if request.lat is None or request.lon is None:
        return _default(spec)
    if not isinstance(raw, str):
        return _default(spec)
    origin_lat, origin_lon, origin_ms = parse_geo(raw)
    speed = velocity_kmh(
        origin_lat,
        origin_lon,
        origin_ms,
        request.lat,
        request.lon,
        to_epoch_ms(request.ts),
    )
    if speed is None:
        return _default(spec)
    return FeatureValue(value=speed, status=FeatureStatus.fresh)


async def _geo_raw(redis: Redis, request: FeatureRequest) -> object:
    if request.lat is None or request.lon is None:
        return None
    return await _await(redis.get(geo_key(request.card_token)))


async def serve(
    redis: Redis,
    request: FeatureRequest,
    *,
    now: datetime,
    n_partitions: int,
) -> dict[str, FeatureValue]:
    """Every catalog feature for the card and merchant at ``now``.

    Watermarks, card window, merchant window and geo are fetched concurrently
    (one Lua EVAL for the merchant sum) to cut Redis RTT stacking.

    ``request.ts`` is not the window clock. Geo-velocity uses it only as the
    time of the request coordinates.
    """
    now_ms = to_epoch_ms(now)
    partition = partition_for(request.card_token, n_partitions)
    watermarks, card_values, merchant_values, geo_raw = await asyncio.gather(
        _watermarks(redis, n_partitions),
        card_stream_features(redis, request.card_token, now_ms),
        merchant_stream_features(redis, request.merchant_id, now_ms),
        _geo_raw(redis, request),
    )
    card = _card_from_values(
        card_values,
        as_of_ms=watermarks[partition],
        now_ms=now_ms,
    )
    merchant = _merchant_from_values(
        merchant_values,
        watermarks=watermarks,
        now_ms=now_ms,
    )
    geo_spec = next(spec for spec in CATALOG if spec.agg == "geo_velocity")
    geo = _geo_from_raw(geo_raw, request, geo_spec)
    ordered: dict[str, FeatureValue] = {}
    for spec in CATALOG:
        if spec.agg == "geo_velocity":
            ordered[spec.name] = geo
        elif spec.entity == "card":
            ordered[spec.name] = card[spec.name]
        else:
            ordered[spec.name] = merchant[spec.name]
    return ordered
