from datetime import timedelta

from fakeredis import FakeAsyncRedis
from support import BASE, make_event

from features.windows import (
    DAY_MS,
    MERCHANT_TTL_S,
    card_key,
    format_geo,
    geo_key,
    hour_bucket,
    member,
    merchant_features_key,
    minute_bucket,
    to_epoch_ms,
    watermark_key,
)
from features.worker import apply_event, load_script


def _at(seconds: int) -> object:
    return BASE + timedelta(seconds=seconds)


async def test_apply_writes_card_merchant_geo_and_watermark() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    event = make_event(ingest_time=_at(2), amount_minor=180, status="declined")
    now_ms = to_epoch_ms(BASE) + 5_000

    result = await apply_event(redis, sha, event, partition=2, offset=7, now_ms=now_ms)

    assert result == "applied"
    event_ms = to_epoch_ms(event.event_time)
    assert await redis.zrange(card_key(event.card_token), 0, -1) == [member(event.event_id, 180)]
    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(event_ms)
    hour = hour_bucket(event_ms)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert await redis.hget(feat, f"m:{minute}:s") == "180"
    assert await redis.hget(feat, f"m:{minute}:d") == "1"
    assert await redis.hget(feat, f"h:{hour}:c") == "1"
    assert await redis.hget(feat, f"h:{hour}:d") == "1"
    assert await redis.get(geo_key(event.card_token)) == format_geo(52.5, 13.4, event_ms)
    assert await redis.hgetall(watermark_key(2)) == {
        "offset": "7",
        "as_of_ms": str(to_epoch_ms(event.ingest_time)),
    }


async def test_apply_skips_offset_at_or_below_watermark() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    first = make_event()
    now_ms = to_epoch_ms(BASE)
    assert await apply_event(redis, sha, first, partition=0, offset=5, now_ms=now_ms) == "applied"

    again = await apply_event(redis, sha, make_event(), partition=0, offset=5, now_ms=now_ms)
    older = await apply_event(redis, sha, make_event(), partition=0, offset=4, now_ms=now_ms)
    assert again == "dup"
    assert older == "dup"
    feat = merchant_features_key(first.merchant_id)
    minute = minute_bucket(now_ms)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert await redis.hget(feat, f"h:{hour_bucket(now_ms)}:c") == "1"

    nxt = await apply_event(redis, sha, make_event(), partition=0, offset=6, now_ms=now_ms)
    assert nxt == "applied"
    assert await redis.hget(feat, f"m:{minute}:c") == "2"
    assert await redis.hget(feat, f"h:{hour_bucket(now_ms)}:c") == "2"


async def test_bucket_ttl_is_set_on_first_touch_only() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now_ms = to_epoch_ms(BASE)
    event = make_event()
    await apply_event(redis, sha, event, partition=1, offset=0, now_ms=now_ms)
    feat = merchant_features_key(event.merchant_id)
    ttl = await redis.ttl(feat)
    assert 0 < ttl <= MERCHANT_TTL_S

    await redis.expire(feat, 10)
    await apply_event(redis, sha, make_event(), partition=1, offset=1, now_ms=now_ms)

    assert await redis.ttl(feat) <= 10


async def test_card_member_older_than_24h_is_trimmed() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now_ms = 1_800_000_000_000
    old_ms = now_ms - DAY_MS - 60_000
    event = make_event(
        event_time=from_ms(old_ms),
        ingest_time=from_ms(now_ms),
        lat=None,
        lon=None,
    )

    assert await apply_event(redis, sha, event, partition=0, offset=0, now_ms=now_ms) == "applied"

    assert await redis.zcard(card_key(event.card_token)) == 0
    assert await redis.get(geo_key(event.card_token)) is None
    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(old_ms)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"


def from_ms(epoch_ms: int) -> object:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(epoch_ms / 1000, UTC)
