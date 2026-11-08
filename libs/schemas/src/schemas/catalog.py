from datetime import timedelta
from typing import Literal, Self

from pydantic import ConfigDict, model_validator

from schemas.coords import StrictModel

Entity = Literal["card", "merchant"]
Kind = Literal["stream", "on_demand"]
Agg = Literal["count", "sum", "decline_rate", "geo_velocity"]

_MAX_WINDOW = timedelta(hours=24)
_MINUTE = timedelta(minutes=1)
_HOUR = timedelta(hours=1)
_DAY = timedelta(hours=24)
_STALE_CARD = timedelta(seconds=5)
_STALE_MERCHANT = timedelta(seconds=30)


class FeatureSpec(StrictModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    entity: Entity
    kind: Kind
    window: timedelta | None
    agg: Agg
    stale_after: timedelta | None
    default: float | int | None

    @model_validator(mode="after")
    def kind_matches_windows(self) -> Self:
        if self.kind == "stream":
            if self.window is None or self.stale_after is None:
                msg = "stream features require window and stale_after"
                raise ValueError(msg)
            if self.window > _MAX_WINDOW:
                msg = "window must not exceed 24 hours"
                raise ValueError(msg)
        elif self.window is not None or self.stale_after is not None:
            msg = "on_demand features must not set window or stale_after"
            raise ValueError(msg)
        return self


CATALOG: tuple[FeatureSpec, ...] = (
    FeatureSpec(
        name="card_cnt_1m",
        entity="card",
        kind="stream",
        window=_MINUTE,
        agg="count",
        stale_after=_STALE_CARD,
        default=0,
    ),
    FeatureSpec(
        name="card_cnt_1h",
        entity="card",
        kind="stream",
        window=_HOUR,
        agg="count",
        stale_after=_STALE_CARD,
        default=0,
    ),
    FeatureSpec(
        name="card_cnt_24h",
        entity="card",
        kind="stream",
        window=_DAY,
        agg="count",
        stale_after=_STALE_CARD,
        default=0,
    ),
    FeatureSpec(
        name="card_sum_1m",
        entity="card",
        kind="stream",
        window=_MINUTE,
        agg="sum",
        stale_after=_STALE_CARD,
        default=0,
    ),
    FeatureSpec(
        name="card_sum_1h",
        entity="card",
        kind="stream",
        window=_HOUR,
        agg="sum",
        stale_after=_STALE_CARD,
        default=0,
    ),
    FeatureSpec(
        name="card_sum_24h",
        entity="card",
        kind="stream",
        window=_DAY,
        agg="sum",
        stale_after=_STALE_CARD,
        default=0,
    ),
    FeatureSpec(
        name="geo_velocity_kmh",
        entity="card",
        kind="on_demand",
        window=None,
        agg="geo_velocity",
        stale_after=None,
        default=None,
    ),
    FeatureSpec(
        name="merchant_cnt_24h",
        entity="merchant",
        kind="stream",
        window=_DAY,
        agg="count",
        stale_after=_STALE_MERCHANT,
        default=0,
    ),
    FeatureSpec(
        name="merchant_decline_rate_24h",
        entity="merchant",
        kind="stream",
        window=_DAY,
        agg="decline_rate",
        stale_after=_STALE_MERCHANT,
        default=0,
    ),
)

_BY_NAME: dict[str, FeatureSpec] = {spec.name: spec for spec in CATALOG}


def by_name(name: str) -> FeatureSpec:
    return _BY_NAME[name]


def for_entity(entity: Entity) -> tuple[FeatureSpec, ...]:
    return tuple(spec for spec in CATALOG if spec.entity == entity)
