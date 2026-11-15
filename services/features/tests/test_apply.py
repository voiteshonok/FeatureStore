from datetime import timedelta

from fakeredis import FakeAsyncRedis
from support import BASE, make_event

from features.windows import (
    DAY_MS,
    MERCHANT_TTL_S,
    card_key,
    format_geo,
    geo_key,
    member,
    merchant_key,
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
    bucket = merchant_key(event.merchant_id, minute_bucket(event_ms))
    assert await redis.hgetall(bucket) == {"count": "1", "sum": "180", "declined": "1"}
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
    bucket = merchant_key(first.merchant_id, minute_bucket(now_ms))
    assert await redis.hget(bucket, "count") == "1"

    nxt = await apply_event(redis, sha, make_event(), partition=0, offset=6, now_ms=now_ms)
    assert nxt == "applied"
    assert await redis.hget(bucket, "count") == "2"


async def test_bucket_ttl_is_set_on_first_touch_only() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now_ms = to_epoch_ms(BASE)
    event = make_event()
    await apply_event(redis, sha, event, partition=1, offset=0, now_ms=now_ms)
    bucket = merchant_key(event.merchant_id, minute_bucket(now_ms))
    ttl = await redis.ttl(bucket)
    assert 0 < ttl <= MERCHANT_TTL_S

    await redis.expire(bucket, 10)
    await apply_event(redis, sha, make_event(), partition=1, offset=1, now_ms=now_ms)

    assert await redis.ttl(bucket) <= 10


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
    bucket = merchant_key(event.merchant_id, minute_bucket(old_ms))
    assert await redis.hget(bucket, "count") == "1"


def from_ms(epoch_ms: int) -> object:
    from datetime import UTC, datetime

    return datetime.fromtimestamp(epoch_ms / 1000, UTC)
