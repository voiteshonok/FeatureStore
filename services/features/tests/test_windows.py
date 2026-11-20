from uuid import uuid4

from fakeredis import FakeAsyncRedis
from support import at_ms
from support_merchant import seed_merchant_minute

from features.windows import (
    MERCHANT_BUCKETS,
    MINUTE_MS,
    card_key,
    card_stream_features,
    format_geo,
    member,
    merchant_features_key,
    merchant_read_plan,
    merchant_stream_features,
    minute_bucket,
    parse_geo,
    to_epoch_ms,
)


def test_minute_bucket_is_floor_of_unix_minutes() -> None:
    assert minute_bucket(0) == 0
    assert minute_bucket(60_000) == 1
    assert minute_bucket(119_999) == 1


def test_geo_point_round_trip() -> None:
    encoded = format_geo(-52.5, 13.4, 1_781_000_000_000)
    assert parse_geo(encoded) == (-52.5, 13.4, 1_781_000_000_000)


async def test_one_minute_card_count_equals_event_count() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    t_ms = 1_781_000_000_000
    amounts = [10, 20, 30, 40, 50]
    for index, amount in enumerate(amounts):
        await redis.zadd(card_key("c"), {member(uuid4(), amount): t_ms - index * 1000})

    features = await card_stream_features(redis, "c", t_ms)

    assert features["card_cnt_1m"] == len(amounts)
    assert features["card_sum_1m"] == sum(amounts)
    assert features["card_cnt_1h"] == len(amounts)
    assert features["card_cnt_24h"] == len(amounts)


async def test_out_of_order_arrivals_have_the_same_card_windows() -> None:
    t_ms = 1_781_000_000_000
    points = [(uuid4(), 10 + index, t_ms - index * 5_000) for index in range(4)]
    forward = FakeAsyncRedis(decode_responses=True)
    backward = FakeAsyncRedis(decode_responses=True)
    for event_id, amount, score in points:
        await forward.zadd(card_key("c"), {member(event_id, amount): score})
    for event_id, amount, score in reversed(points):
        await backward.zadd(card_key("c"), {member(event_id, amount): score})

    assert await card_stream_features(forward, "c", t_ms) == await card_stream_features(
        backward, "c", t_ms
    )


async def test_duplicate_member_does_not_change_card_values() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    t_ms = 1_781_000_000_000
    key = card_key("c")
    duplicated = member(uuid4(), 40)
    await redis.zadd(key, {duplicated: t_ms, member(uuid4(), 15): t_ms - 1000})
    await redis.zadd(key, {duplicated: t_ms - 5})

    features = await card_stream_features(redis, "c", t_ms)

    assert features["card_cnt_1m"] == 2
    assert features["card_sum_1m"] == 55


async def test_event_exactly_one_minute_back_is_outside_the_window() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    t_ms = 1_781_000_000_000
    key = card_key("c")
    await redis.zadd(
        key,
        {
            member(uuid4(), 7): t_ms - MINUTE_MS,
            member(uuid4(), 9): t_ms - MINUTE_MS + 1,
        },
    )

    features = await card_stream_features(redis, "c", t_ms)

    assert features["card_cnt_1m"] == 1
    assert features["card_sum_1m"] == 9
    assert features["card_cnt_1h"] == 2
    assert features["card_sum_1h"] == 16


def test_merchant_read_plan_uses_hours_and_edge_minutes() -> None:
    # Window covering >2 hours: full middle hours + partial edges.
    current = 10_000  # 10000 % 60 == 40 → current hour incomplete
    first = current - MERCHANT_BUCKETS + 1
    hours, minutes = merchant_read_plan(first, current)
    assert hours
    assert minutes
    assert max(hours) == current // 60 - 1
    assert current in minutes
    covered = set(minutes)
    for hour in hours:
        covered.update(range(hour * 60, hour * 60 + 60))
    assert covered == set(range(first, current + 1))
    assert len(hours) + len(minutes) < MERCHANT_BUCKETS


def test_merchant_read_plan_complete_current_hour_is_hourly() -> None:
    current = 10_019  # last minute of hour 166
    first = current - MERCHANT_BUCKETS + 1
    hours, minutes = merchant_read_plan(first, current)
    assert current // 60 in hours
    assert current not in minutes


async def test_merchant_window_uses_1440_minute_buckets() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    t_ms = to_epoch_ms(at_ms(1_781_000_000_000))
    current = minute_bucket(t_ms)
    first = current - MERCHANT_BUCKETS + 1
    await seed_merchant_minute(
        redis, "m", current, first=first, current=current, count=2, declined=1
    )
    await seed_merchant_minute(
        redis, "m", current - 1439, first=first, current=current, count=3, declined=0
    )
    # Outside the window — must not count (write onto minute field directly).
    key = merchant_features_key("m")
    await redis.hset(
        key,
        mapping={
            f"m:{current - 1440}:c": 9,
            f"m:{current - 1440}:d": 9,
        },
    )

    features = await merchant_stream_features(redis, "m", t_ms)

    assert features["merchant_cnt_24h"] == 5
    assert features["merchant_decline_rate_24h"] == 0.2


async def test_empty_merchant_decline_rate_is_zero() -> None:
    redis = FakeAsyncRedis(decode_responses=True)

    features = await merchant_stream_features(redis, "nobody", 1_781_000_000_000)

    assert features["merchant_cnt_24h"] == 0
    assert features["merchant_decline_rate_24h"] == 0.0
    assert type(features["merchant_decline_rate_24h"]) is float
