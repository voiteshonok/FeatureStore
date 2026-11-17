"""Sample Redis memory for the K1/K2 experiment."""

from __future__ import annotations

import statistics
from typing import Any

import redis


def capture_redis_memory(
    *,
    url: str = "redis://localhost:6379/0",
    card_sample: int = 1000,
    mbkt_sample: int = 1000,
) -> dict[str, Any]:
    """INFO memory, DBSIZE, and MEMORY USAGE samples for card:* / mbkt:*."""
    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        info = client.info("memory")
        dbsize = int(client.dbsize())
        card_sizes = _sample_usage(client, "card:*", card_sample)
        mbkt_sizes = _sample_usage(client, "mbkt:*", mbkt_sample)
    finally:
        client.close()
    used = int(info.get("used_memory", 0))
    return {
        "used_memory": used,
        "used_memory_human": str(info.get("used_memory_human", "")),
        "used_memory_rss": int(info.get("used_memory_rss", 0) or 0),
        "maxmemory": int(info.get("maxmemory", 0) or 0),
        "dbsize": dbsize,
        "adr_002_estimate_gb": 1.5,
        "used_memory_gb": used / (1024**3),
        "card_keys": _sample_stats(card_sizes),
        "mbkt_keys": _sample_stats(mbkt_sizes),
        "note": ("ADR-002 ~1.5 GB assumes a day of history; a short warm-up will be lower."),
    }


def _sample_usage(client: Any, pattern: str, limit: int) -> list[int]:
    keys = _scan_keys(client, pattern, limit)
    sizes: list[int] = []
    for key in keys:
        usage = client.memory_usage(key)
        if usage is not None:
            sizes.append(int(usage))
    return sizes


def _scan_keys(client: Any, pattern: str, limit: int) -> list[str]:
    found: list[str] = []
    for key in client.scan_iter(match=pattern, count=500):
        found.append(str(key))
        if len(found) >= limit:
            break
    return found


def _sample_stats(sizes: list[int]) -> dict[str, float | int]:
    if not sizes:
        return {"count": 0, "sum_bytes": 0, "p50_bytes": 0, "p99_bytes": 0, "max_bytes": 0}
    ordered = sorted(sizes)
    return {
        "count": len(sizes),
        "sum_bytes": sum(sizes),
        "p50_bytes": int(statistics.median(ordered)),
        "p99_bytes": ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))],
        "max_bytes": ordered[-1],
    }
