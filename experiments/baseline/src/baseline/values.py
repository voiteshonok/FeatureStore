"""Map aggregate rows to catalog ``FeatureValue``s.

The stand returns ``fresh`` when the entity has rows in its 24 h window and
``default`` otherwise. It does not return ``stale`` or ``fallback`` (ADR-015).
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

from baseline.geo import velocity_kmh
from baseline.windows import to_epoch_ms
from schemas import (
    CATALOG,
    FeatureRequest,
    FeatureSpec,
    FeatureStatus,
    FeatureValue,
    by_name,
    for_entity,
)

_MINUTE = timedelta(minutes=1)
_HOUR = timedelta(hours=1)
_DAY = timedelta(hours=24)


@dataclass(frozen=True)
class CardAggregates:
    """One card, three windows, counted and summed inside ``(T - window, T]``."""

    cnt_1m: int
    cnt_1h: int
    cnt_24h: int
    sum_1m: int
    sum_1h: int
    sum_24h: int


@dataclass(frozen=True)
class MerchantAggregates:
    """One merchant over the last 1,440 UTC minute buckets."""

    count: int
    declined: int


@dataclass(frozen=True)
class GeoPoint:
    """Latest card point that carried coordinates."""

    lat: float
    lon: float
    event_time: datetime


def _default(spec: FeatureSpec) -> FeatureValue:
    return FeatureValue(value=spec.default, status=FeatureStatus.default)


def _fresh(value: int | float, now: datetime) -> FeatureValue:
    return FeatureValue(value=value, as_of=now, age_ms=0, status=FeatureStatus.fresh)


def _card_value(spec: FeatureSpec, row: CardAggregates) -> int:
    key = (spec.agg, spec.window)
    table: dict[tuple[str, timedelta | None], int] = {
        ("count", _MINUTE): row.cnt_1m,
        ("count", _HOUR): row.cnt_1h,
        ("count", _DAY): row.cnt_24h,
        ("sum", _MINUTE): row.sum_1m,
        ("sum", _HOUR): row.sum_1h,
        ("sum", _DAY): row.sum_24h,
    }
    try:
        return table[key]
    except KeyError as exc:
        msg = f"unsupported card feature {spec.name}"
        raise ValueError(msg) from exc


def _merchant_value(spec: FeatureSpec, row: MerchantAggregates) -> int | float:
    if spec.agg == "count":
        return row.count
    if spec.agg == "decline_rate":
        return row.declined / row.count
    msg = f"unsupported merchant aggregation {spec.agg}"
    raise ValueError(msg)


def card_features(row: CardAggregates, now: datetime) -> dict[str, FeatureValue]:
    """Card stream features. An empty 24 h window is the catalog default."""
    specs = tuple(spec for spec in for_entity("card") if spec.kind == "stream")
    if row.cnt_24h == 0:
        return {spec.name: _default(spec) for spec in specs}
    return {spec.name: _fresh(_card_value(spec, row), now) for spec in specs}


def merchant_features(row: MerchantAggregates, now: datetime) -> dict[str, FeatureValue]:
    """Merchant stream features. An empty bucket window is the catalog default."""
    specs = tuple(spec for spec in for_entity("merchant") if spec.kind == "stream")
    if row.count == 0:
        return {spec.name: _default(spec) for spec in specs}
    return {spec.name: _fresh(_merchant_value(spec, row), now) for spec in specs}


def geo_feature(point: GeoPoint | None, request: FeatureRequest) -> FeatureValue:
    """Velocity from the latest stored point to the request coordinates.

    ``request.ts`` is the time of those coordinates. A missing point, missing
    request coordinates, or a zero time span is the catalog default.
    """
    spec = by_name("geo_velocity_kmh")
    if request.lat is None or request.lon is None or point is None:
        return _default(spec)
    speed = velocity_kmh(
        point.lat,
        point.lon,
        to_epoch_ms(point.event_time),
        request.lat,
        request.lon,
        to_epoch_ms(request.ts),
    )
    if speed is None:
        return _default(spec)
    return FeatureValue(value=speed, status=FeatureStatus.fresh)


def assemble(
    card: CardAggregates | None,
    merchant: MerchantAggregates | None,
    point: GeoPoint | None,
    request: FeatureRequest,
    now: datetime,
) -> dict[str, FeatureValue]:
    """Catalog features for the entities on the request, in catalog order."""
    cards = card_features(card, now) if card is not None else None
    merchants = merchant_features(merchant, now) if merchant is not None else None
    geo = geo_feature(point, request) if request.card_token is not None else None
    ordered: dict[str, FeatureValue] = {}
    for spec in CATALOG:
        if spec.agg == "geo_velocity":
            if geo is not None:
                ordered[spec.name] = geo
        elif spec.entity == "card":
            if cards is not None:
                ordered[spec.name] = cards[spec.name]
        elif merchants is not None:
            ordered[spec.name] = merchants[spec.name]
    return ordered
