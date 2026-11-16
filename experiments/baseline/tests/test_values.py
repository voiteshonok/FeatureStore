from datetime import UTC, datetime, timedelta

from baseline.geo import velocity_kmh
from baseline.values import (
    CardAggregates,
    GeoPoint,
    MerchantAggregates,
    assemble,
    card_features,
    geo_feature,
    merchant_features,
)
from baseline.windows import to_epoch_ms
from schemas import CATALOG, FeatureRequest, FeatureStatus, for_entity

_T = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
_EQUATOR_DEGREE_KMH = 111.19492664455873


def _request(**overrides: object) -> FeatureRequest:
    payload: dict[str, object] = {
        "card_token": "c1",
        "merchant_id": "m1",
        "ts": _T,
    }
    payload.update(overrides)
    return FeatureRequest.model_validate(payload)


def test_unknown_card_is_the_catalog_default() -> None:
    values = card_features(CardAggregates(5, 5, 0, 9, 9, 9), _T)
    for spec in for_entity("card"):
        if spec.kind != "stream":
            continue
        feature = values[spec.name]
        assert feature.status is FeatureStatus.default
        assert feature.value == spec.default
        assert feature.as_of is None
        assert feature.age_ms is None


def test_empty_minute_inside_a_populated_day_is_fresh_zero() -> None:
    values = card_features(CardAggregates(0, 2, 3, 0, 40, 90), _T)
    minute = values["card_cnt_1m"]
    assert minute.status is FeatureStatus.fresh
    assert minute.value == 0
    assert type(minute.value) is int
    assert minute.age_ms == 0
    assert minute.as_of == _T
    assert values["card_sum_24h"].value == 90
    assert type(values["card_sum_24h"].value) is int


def test_empty_merchant_is_default_and_a_rate_is_a_float() -> None:
    empty = merchant_features(MerchantAggregates(0, 4), _T)
    assert empty["merchant_cnt_24h"].status is FeatureStatus.default
    assert empty["merchant_cnt_24h"].value == 0
    assert empty["merchant_decline_rate_24h"].status is FeatureStatus.default
    assert empty["merchant_decline_rate_24h"].value == 0

    filled = merchant_features(MerchantAggregates(4, 1), _T)
    rate = filled["merchant_decline_rate_24h"]
    assert rate.status is FeatureStatus.fresh
    assert rate.value == 0.25
    assert type(rate.value) is float
    assert filled["merchant_cnt_24h"].value == 4
    assert type(filled["merchant_cnt_24h"].value) is int


def test_geo_without_a_point_or_request_coordinates_is_default() -> None:
    missing_point = geo_feature(None, _request(lat=0.0, lon=1.0))
    assert missing_point.status is FeatureStatus.default
    assert missing_point.value is None

    point = GeoPoint(0.0, 0.0, _T)
    missing_coords = geo_feature(point, _request())
    assert missing_coords.status is FeatureStatus.default
    assert missing_coords.value is None


def test_geo_zero_span_is_default_and_a_move_is_fresh() -> None:
    point = GeoPoint(0.0, 0.0, _T)
    same = geo_feature(point, _request(lat=0.0, lon=1.0, ts=_T))
    assert same.status is FeatureStatus.default
    assert same.value is None

    moved = geo_feature(point, _request(lat=0.0, lon=1.0, ts=_T + timedelta(hours=1)))
    assert moved.status is FeatureStatus.fresh
    assert moved.as_of is None
    assert moved.age_ms is None
    assert moved.value == _EQUATOR_DEGREE_KMH
    origin_ms = to_epoch_ms(_T)
    assert velocity_kmh(0.0, 0.0, origin_ms, 0.0, 1.0, origin_ms + 3_600_000) == moved.value


def test_assemble_follows_catalog_order_and_skips_stale() -> None:
    features = assemble(
        CardAggregates(1, 1, 1, 10, 10, 10),
        MerchantAggregates(2, 1),
        None,
        _request(),
        _T,
    )
    assert list(features) == [spec.name for spec in CATALOG]
    assert {item.status for item in features.values()} <= {
        FeatureStatus.fresh,
        FeatureStatus.default,
    }
    assert features["geo_velocity_kmh"].status is FeatureStatus.default
