from datetime import timedelta

from fakeredis import FakeAsyncRedis
from support import BASE

from features.freshness import serve
from features.geo import velocity_kmh
from features.windows import format_geo, geo_key, to_epoch_ms
from schemas import FeatureRequest, FeatureStatus

# One degree of longitude at the equator, over one hour, on a 6371 km sphere.
_EQUATOR_DEGREE_KMH = 111.19492664455873


def _request(**overrides: object) -> FeatureRequest:
    payload: dict[str, object] = {
        "card_token": "card-geo",
        "merchant_id": "m-1",
        "ts": BASE,
    }
    payload.update(overrides)
    return FeatureRequest.model_validate(payload)


def test_velocity_for_one_equatorial_degree_in_one_hour() -> None:
    origin_ms = to_epoch_ms(BASE)
    speed = velocity_kmh(0.0, 0.0, origin_ms, 0.0, 1.0, origin_ms + 3_600_000)
    assert speed == _EQUATOR_DEGREE_KMH


def test_zero_time_span_has_no_velocity() -> None:
    assert velocity_kmh(0.0, 0.0, 1_000, 0.0, 1.0, 1_000) is None


async def test_missing_coordinates_or_stored_point_is_default() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    no_coords = await serve(redis, _request(), now=BASE, n_partitions=4)
    assert no_coords["geo_velocity_kmh"].status is FeatureStatus.default
    assert no_coords["geo_velocity_kmh"].value is None
    assert no_coords["geo_velocity_kmh"].as_of is None

    with_coords = await serve(
        redis,
        _request(lat=0.0, lon=1.0, ts=BASE + timedelta(hours=1)),
        now=BASE,
        n_partitions=4,
    )
    assert with_coords["geo_velocity_kmh"].status is FeatureStatus.default


async def test_zero_time_span_against_the_stored_point_is_default() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    origin_ms = to_epoch_ms(BASE)
    await redis.set(geo_key("card-geo"), format_geo(0.0, 0.0, origin_ms))

    features = await serve(
        redis,
        _request(lat=0.0, lon=1.0, ts=BASE),
        now=BASE,
        n_partitions=4,
    )

    assert features["geo_velocity_kmh"].status is FeatureStatus.default
    assert features["geo_velocity_kmh"].value is None


async def test_stored_point_returns_the_haversine_velocity() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    origin_ms = to_epoch_ms(BASE)
    await redis.set(geo_key("card-geo"), format_geo(0.0, 0.0, origin_ms))
    features = await serve(
        redis,
        _request(lat=0.0, lon=1.0, ts=BASE + timedelta(hours=1)),
        now=BASE + timedelta(hours=1),
        n_partitions=4,
    )
    geo = features["geo_velocity_kmh"]
    assert geo.status is FeatureStatus.fresh
    assert geo.value == _EQUATOR_DEGREE_KMH
    assert geo.as_of is None
    assert geo.age_ms is None
