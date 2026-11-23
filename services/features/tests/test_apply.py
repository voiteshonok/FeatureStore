from datetime import UTC, datetime, timedelta
from uuid import uuid4

from fakeredis import FakeAsyncRedis
from support import BASE, fixed_id, make_event

from features.windows import (
    DAY_MS,
    MINUTE_MS,
    card_key,
    format_geo,
    geo_key,
    hour_bucket,
    hour_expiry_ms,
    key_expiry_ms,
    member,
    merchant_features_key,
    merchant_stream_features,
    minute_bucket,
    minute_expiry_ms,
    to_epoch_ms,
    watermark_key,
)
from features.worker import apply_event, load_script


def _at(seconds: int) -> datetime:
    return BASE + timedelta(seconds=seconds)


def from_ms(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC)


async def _hexpiretime(redis: FakeAsyncRedis, key: str, *fields: str) -> list[int]:
    raw = await redis.execute_command("HPEXPIRETIME", key, "FIELDS", len(fields), *fields)
    assert isinstance(raw, list)
    return [int(item) for item in raw]


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


async def test_field_and_key_expiry_from_event_time() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now_ms = to_epoch_ms(BASE)
    event = make_event()
    await apply_event(redis, sha, event, partition=1, offset=0, now_ms=now_ms)

    event_ms = to_epoch_ms(event.event_time)
    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(event_ms)
    hour = hour_bucket(event_ms)
    assert await redis.ttl(feat) == -1
    assert (
        await _hexpiretime(redis, feat, f"m:{minute}:c", f"m:{minute}:s", f"m:{minute}:d")
        == [minute_expiry_ms(event_ms)] * 3
    )
    assert (
        await _hexpiretime(redis, feat, f"h:{hour}:c", f"h:{hour}:s", f"h:{hour}:d")
        == [hour_expiry_ms(event_ms)] * 3
    )
    assert await redis.execute_command("PEXPIRETIME", card_key(event.card_token)) == key_expiry_ms(
        event_ms
    )
    assert await redis.execute_command("PEXPIRETIME", geo_key(event.card_token)) == key_expiry_ms(
        event_ms
    )


async def test_event_older_than_25h_leaves_no_window_keys() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now = datetime.now(UTC).replace(microsecond=0)
    now_ms = to_epoch_ms(now)
    # Hour fields live until the end of the event's hour, plus 24h and 1min.
    # That is up to 25h + 1min after event_time, so exactly 25h still leaves
    # the merchant hash during the first minute of an hour.
    old_ms = now_ms - DAY_MS - 61 * MINUTE_MS - 1
    event = make_event(
        event_time=from_ms(old_ms),
        ingest_time=now,
        lat=52.5,
        lon=13.4,
        card_token="quiet-card",  # noqa: S106
        merchant_id="quiet-m",
    )

    assert await apply_event(redis, sha, event, partition=0, offset=0, now_ms=now_ms) == "applied"

    assert await redis.exists(card_key(event.card_token)) == 0
    assert await redis.exists(geo_key(event.card_token)) == 0
    assert await redis.exists(merchant_features_key(event.merchant_id)) == 0


async def test_late_event_does_not_shorten_card_or_geo_expiry() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now = datetime.now(UTC).replace(microsecond=0)
    now_ms = to_epoch_ms(now)
    first = make_event(
        event_time=now,
        ingest_time=now,
        card_token="c-late",  # noqa: S106
        merchant_id="m-late",
    )
    await apply_event(redis, sha, first, partition=0, offset=0, now_ms=now_ms)
    card_before = int(await redis.execute_command("PEXPIRETIME", card_key(first.card_token)))
    geo_before = int(await redis.execute_command("PEXPIRETIME", geo_key(first.card_token)))

    late = make_event(
        event_time=now - timedelta(hours=2),
        ingest_time=now,
        card_token=first.card_token,
        merchant_id=first.merchant_id,
    )
    assert await apply_event(redis, sha, late, partition=0, offset=1, now_ms=now_ms) == "applied"

    assert (
        int(await redis.execute_command("PEXPIRETIME", card_key(first.card_token))) == card_before
    )
    assert int(await redis.execute_command("PEXPIRETIME", geo_key(first.card_token))) == geo_before


async def test_same_event_id_new_offset_is_dup_event() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now_ms = to_epoch_ms(BASE)
    event = make_event(event_id=fixed_id(), amount_minor=250, status="declined")
    assert await apply_event(redis, sha, event, partition=3, offset=10, now_ms=now_ms) == "applied"
    feat = merchant_features_key(event.merchant_id)
    minute = minute_bucket(now_ms)
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert await redis.hget(feat, f"m:{minute}:d") == "1"

    again = await apply_event(redis, sha, event, partition=3, offset=11, now_ms=now_ms)
    assert again == "dup_event"
    assert await redis.hget(feat, f"m:{minute}:c") == "1"
    assert await redis.hget(feat, f"m:{minute}:d") == "1"
    assert await redis.hgetall(watermark_key(3)) == {
        "offset": "11",
        "as_of_ms": str(to_epoch_ms(event.ingest_time)),
    }


async def test_stream_window_count_matches_last_1440_minutes() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now = datetime.now(UTC).replace(microsecond=0)
    now_ms = to_epoch_ms(now)
    merchant_id = "m-window"
    card_token = "c-window"  # noqa: S105
    in_window = 0
    offset = 0
    for hours_ago in range(26, -1, -1):
        event_time = now - timedelta(hours=hours_ago)
        event = make_event(
            event_id=uuid4(),
            event_time=event_time,
            ingest_time=now,
            merchant_id=merchant_id,
            card_token=card_token,
            lat=None,
            lon=None,
        )
        assert (
            await apply_event(redis, sha, event, partition=0, offset=offset, now_ms=now_ms)
            == "applied"
        )
        offset += 1
        if to_epoch_ms(event_time) > now_ms - DAY_MS:
            in_window += 1

    features = await merchant_stream_features(redis, merchant_id, now_ms)
    assert features["merchant_cnt_24h"] == in_window


async def test_all_events_older_than_25h_leave_no_window_keys() -> None:
    redis = FakeAsyncRedis(decode_responses=True)
    sha = await load_script(redis)
    now = datetime.now(UTC).replace(microsecond=0)
    now_ms = to_epoch_ms(now)
    for index in range(5):
        event_time = now - timedelta(hours=26, minutes=index)
        event = make_event(
            event_id=uuid4(),
            event_time=event_time,
            ingest_time=now,
            merchant_id=f"m-old-{index}",
            card_token=f"c-old-{index}",
            lat=1.0,
            lon=2.0,
        )
        assert (
            await apply_event(redis, sha, event, partition=0, offset=index, now_ms=now_ms)
            == "applied"
        )
        assert await redis.exists(card_key(event.card_token)) == 0
        assert await redis.exists(geo_key(event.card_token)) == 0
        assert await redis.exists(merchant_features_key(event.merchant_id)) == 0
