"""Helpers to seed ADR-016 ``mfeat:{id}`` hashes in unit tests."""

from redis.asyncio import Redis

from features.windows import merchant_features_key, merchant_read_plan


async def seed_merchant_minute(
    redis: Redis,
    merchant_id: str,
    minute: int,
    *,
    first: int,
    current: int,
    count: int,
    declined: int,
) -> None:
    """Write counts onto the fields ``merchant_read_plan`` will read."""
    key = merchant_features_key(merchant_id)
    hours, minutes = merchant_read_plan(first, current)
    hour = minute // 60
    if hour in hours:
        prev_c = int(await redis.hget(key, f"h:{hour}:c") or 0)
        prev_d = int(await redis.hget(key, f"h:{hour}:d") or 0)
        await redis.hset(
            key,
            mapping={
                f"h:{hour}:c": prev_c + count,
                f"h:{hour}:d": prev_d + declined,
            },
        )
        return
    if minute in minutes:
        await redis.hset(
            key,
            mapping={
                f"m:{minute}:c": count,
                f"m:{minute}:d": declined,
            },
        )


async def seed_merchant_event(
    redis: Redis,
    merchant_id: str,
    minute: int,
    *,
    count: int = 1,
    declined: int = 0,
    amount: int = 0,
) -> None:
    """Dual-write minute + hour fields like apply.lua."""
    key = merchant_features_key(merchant_id)
    hour = minute // 60
    await redis.hincrby(key, f"m:{minute}:c", count)
    await redis.hincrby(key, f"m:{minute}:d", declined)
    await redis.hincrby(key, f"m:{minute}:s", amount)
    await redis.hincrby(key, f"h:{hour}:c", count)
    await redis.hincrby(key, f"h:{hour}:d", declined)
    await redis.hincrby(key, f"h:{hour}:s", amount)
