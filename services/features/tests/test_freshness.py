from datetime import timedelta
from uuid import uuid4

from fakeredis import FakeAsyncRedis
from support import BASE
from support_merchant import seed_merchant_event

from features.freshness import from_epoch_ms, serve
from features.windows import (
    card_key,
    member,
    minute_bucket,
    stream_specs,
    to_epoch_ms,
    watermark_key,
)
from schemas import CATALOG, FeatureRequest, FeatureStatus, partition_for

_CARD = "card-fresh"
_MERCHANT = "m-1"
_PARTITIONS = 4
_CARD_STALE_MS = 5_000
_MERCHANT_STALE_MS = 30_000


def _request(**overrides: object) -> FeatureRequest:
    payload: dict[str, object] = {
        "card_token": _CARD,
        "merchant_id": _MERCHANT,
        "ts": BASE,
    }
    payload.update(overrides)
    return FeatureRequest.model_validate(payload)


async def _watermarks(
    redis: FakeAsyncRedis,
    as_of_ms: int,
    overrides: dict[int, int] | None = None,
) -> None:
    chosen = overrides or {}
    for partition in range(_PARTITIONS):
        stamp = chosen.get(partition, as_of_ms)
        await redis.hset(
            watermark_key(partition),
            mapping={"offset": "1", "as_of_ms": str(stamp)},
        )


async def _card_event(redis: FakeAsyncRedis, when_ms: int, amount: int = 40) -> None:
    await redis.zadd(card_key(_CARD), {member(uuid4(), amount): when_ms})


async def _merchant_bucket(redis: FakeAsyncRedis, when_ms: int) -> None:
    await seed_merchant_event(
        redis,
        _MERCHANT,
        minute_bucket(when_ms),
        count=2,
        declined=1,
        amount=80,
    )


def _card_names() -> set[str]:
    return {spec.name for spec in stream_specs("card")}


def _merchant_names() -> set[str]:
    return {spec.name for spec in stream_specs("merchant")}


async def test_exact_stale_after_is_fresh_and_one_millisecond_over_is_stale() -> None:
    now_ms = to_epoch_ms(BASE)
    redis = FakeAsyncRedis(decode_responses=True)
    await _card_event(redis, now_ms)
    await _merchant_bucket(redis, now_ms)
    await _watermarks(redis, now_ms - _CARD_STALE_MS)

    fresh = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)
    for name in _card_names():
        assert fresh[name].status is FeatureStatus.fresh
        assert fresh[name].age_ms == _CARD_STALE_MS
    for name in _merchant_names():
        assert fresh[name].status is FeatureStatus.fresh

    await _watermarks(redis, now_ms - _CARD_STALE_MS - 1)
    stale = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)
    for name in _card_names():
        assert stale[name].status is FeatureStatus.stale
        assert stale[name].age_ms == _CARD_STALE_MS + 1

    await _watermarks(redis, now_ms - _MERCHANT_STALE_MS)
    merchant_fresh = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)
    for name in _merchant_names():
        assert merchant_fresh[name].status is FeatureStatus.fresh
        assert merchant_fresh[name].age_ms == _MERCHANT_STALE_MS

    await _watermarks(redis, now_ms - _MERCHANT_STALE_MS - 1)
    merchant_stale = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)
    for name in _merchant_names():
        assert merchant_stale[name].status is FeatureStatus.stale
        assert merchant_stale[name].age_ms == _MERCHANT_STALE_MS + 1


async def test_entity_without_data_is_default() -> None:
    now_ms = to_epoch_ms(BASE)
    redis = FakeAsyncRedis(decode_responses=True)
    await _watermarks(redis, now_ms)

    features = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)

    for name in _card_names() | _merchant_names():
        assert features[name].status is FeatureStatus.default
        assert features[name].as_of is None
        assert features[name].age_ms is None
        assert features[name].value == 0


async def test_card_as_of_is_its_partition_and_merchant_as_of_is_the_oldest() -> None:
    now_ms = to_epoch_ms(BASE)
    redis = FakeAsyncRedis(decode_responses=True)
    await _card_event(redis, now_ms, amount=15)
    await _merchant_bucket(redis, now_ms)
    partition = partition_for(_CARD, _PARTITIONS)
    lagging = (partition + 1) % _PARTITIONS
    await _watermarks(
        redis,
        now_ms,
        {partition: now_ms, lagging: now_ms - _MERCHANT_STALE_MS - 1},
    )

    features = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)

    for name in _card_names():
        assert features[name].status is FeatureStatus.fresh
        assert features[name].as_of == from_epoch_ms(now_ms)
        assert features[name].age_ms == 0
    assert features["card_sum_1m"].value == 15
    for name in _merchant_names():
        assert features[name].status is FeatureStatus.stale
        assert features[name].as_of == from_epoch_ms(now_ms - _MERCHANT_STALE_MS - 1)


async def test_missing_watermark_is_default_even_when_events_exist() -> None:
    now_ms = to_epoch_ms(BASE)
    redis = FakeAsyncRedis(decode_responses=True)
    await _card_event(redis, now_ms, amount=99)
    partition = partition_for(_CARD, _PARTITIONS)
    for index in range(_PARTITIONS):
        if index == partition:
            continue
        await redis.hset(
            watermark_key(index),
            mapping={"offset": "1", "as_of_ms": str(now_ms)},
        )

    features = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)

    for name in _card_names():
        assert features[name].status is FeatureStatus.default
        assert features[name].value == 0
        assert features[name].as_of is None


async def test_window_clock_is_now_not_request_ts() -> None:
    now_ms = to_epoch_ms(BASE)
    redis = FakeAsyncRedis(decode_responses=True)
    await _watermarks(redis, now_ms)
    await _card_event(redis, now_ms)
    await _merchant_bucket(redis, now_ms)

    features = await serve(
        redis,
        _request(ts=BASE - timedelta(hours=2)),
        now=BASE,
        n_partitions=_PARTITIONS,
    )

    assert features["card_cnt_1m"].value == 1
    assert features["card_cnt_1m"].status is FeatureStatus.fresh


async def test_one_entity_omits_the_other_side() -> None:
    now_ms = to_epoch_ms(BASE)
    redis = FakeAsyncRedis(decode_responses=True)
    await _watermarks(redis, now_ms)
    await _card_event(redis, now_ms)
    await _merchant_bucket(redis, now_ms)

    card_only = await serve(
        redis,
        _request(merchant_id=None),
        now=BASE,
        n_partitions=_PARTITIONS,
    )
    assert set(card_only) == _card_names() | {"geo_velocity_kmh"}
    assert card_only["card_cnt_1m"].value == 1
    assert card_only["geo_velocity_kmh"].status is FeatureStatus.default

    merchant_only = await serve(
        redis,
        _request(card_token=None),
        now=BASE,
        n_partitions=_PARTITIONS,
    )
    assert set(merchant_only) == _merchant_names()
    assert merchant_only["merchant_cnt_24h"].value == 2

    both = await serve(redis, _request(), now=BASE, n_partitions=_PARTITIONS)
    assert set(both) == {spec.name for spec in CATALOG}
    assert len(both) == 9
