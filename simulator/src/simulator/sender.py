"""Post generated payments to ingestion at a fixed rate."""

import asyncio
import math
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import httpx

from schemas import PaymentIn
from simulator.config import load_config
from simulator.generator import StepClock, generate

_MAX_ATTEMPTS = 5
_BACKOFF_START_S = 0.1
_BACKOFF_CAP_S = 30.0
_REPORT_INTERVAL_S = 10.0


@dataclass
class Stats:
    """Counters for one simulator run."""

    generated: int = 0
    sent: int = 0
    accepted: int = 0
    duplicates: int = 0
    http_422: int = 0
    http_503: int = 0
    retries: int = 0
    failed: int = 0
    elapsed_s: float = 0.0
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def achieved_tps(self) -> float:
        if self.elapsed_s <= 0:
            return 0.0
        return self.generated / self.elapsed_s

    def to_json(self) -> dict[str, float | int]:
        return {
            "generated": self.generated,
            "sent": self.sent,
            "accepted": self.accepted,
            "duplicates": self.duplicates,
            "http_422": self.http_422,
            "http_503": self.http_503,
            "retries": self.retries,
            "failed": self.failed,
            "achieved_tps": self.achieved_tps,
            "latency_p50_ms": _percentile(self.latencies_ms, 50),
            "latency_p95_ms": _percentile(self.latencies_ms, 95),
            "elapsed_s": self.elapsed_s,
        }


class TokenBucket:
    """Admit work at ``rate`` per second, with up to one second of burst."""

    def __init__(self, rate: float) -> None:
        if rate <= 0:
            msg = "tps must be positive"
            raise ValueError(msg)
        self.rate = rate
        self.capacity = rate
        self.tokens = rate
        self.updated = time.monotonic()

    async def acquire(self) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self.updated
            self.updated = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= 1:
                self.tokens -= 1
                return
            await asyncio.sleep((1 - self.tokens) / self.rate)


def parse_duration(text: str) -> float:
    """Parse ``60``, ``60s``, ``10m``, or ``1h`` into seconds."""
    raw = text.strip().lower()
    if not raw:
        msg = "duration is empty"
        raise ValueError(msg)
    unit = raw[-1]
    if unit in "smh":
        number = raw[:-1]
        scale = {"s": 1.0, "m": 60.0, "h": 3600.0}[unit]
    else:
        number = raw
        scale = 1.0
    try:
        seconds = float(number) * scale
    except ValueError as exc:
        msg = f"invalid duration: {text}"
        raise ValueError(msg) from exc
    if seconds <= 0:
        msg = "duration must be positive"
        raise ValueError(msg)
    return seconds


async def run_sender(
    *,
    url: str,
    tps: float,
    duration_s: float,
    seed: int,
    client: httpx.AsyncClient | None = None,
    report: bool = True,
) -> Stats:
    """Generate payments for ``duration_s`` and post them at ``tps``."""
    if client is None:
        async with _client() as owned:
            return await _run(
                owned,
                url=url,
                tps=tps,
                duration_s=duration_s,
                seed=seed,
                report=report,
            )
    return await _run(
        client,
        url=url,
        tps=tps,
        duration_s=duration_s,
        seed=seed,
        report=report,
    )


async def post_payment(
    client: httpx.AsyncClient,
    url: str,
    payment: PaymentIn,
    stats: Stats,
    lock: asyncio.Lock,
) -> None:
    """Post one payment, retrying 503 and connection errors up to five times."""
    body = payment.model_dump(mode="json")
    delay = _BACKOFF_START_S
    honor_header = False
    for attempt in range(_MAX_ATTEMPTS):
        if attempt:
            async with lock:
                stats.retries += 1
            await asyncio.sleep(delay)
            if not honor_header:
                delay = min(delay * 2, _BACKOFF_CAP_S)
            honor_header = False
        started = time.perf_counter()
        try:
            response = await client.post(url, json=body)
        except httpx.TransportError:
            async with lock:
                stats.sent += 1
            continue
        elapsed_ms = (time.perf_counter() - started) * 1000
        async with lock:
            stats.sent += 1
            stats.latencies_ms.append(elapsed_ms)
            if response.status_code == 202:
                if bool(response.json().get("duplicate")):
                    stats.duplicates += 1
                else:
                    stats.accepted += 1
                return
            if response.status_code == 422:
                stats.http_422 += 1
                return
            if response.status_code == 503:
                stats.http_503 += 1
                retry_after = _retry_after(response)
                if retry_after is not None:
                    delay = retry_after
                    honor_header = True
                continue
        async with lock:
            stats.failed += 1
        return
    async with lock:
        stats.failed += 1


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(30.0, connect=5.0),
        limits=httpx.Limits(max_connections=2000, max_keepalive_connections=200),
    )


async def _run(
    client: httpx.AsyncClient,
    *,
    url: str,
    tps: float,
    duration_s: float,
    seed: int,
    report: bool,
) -> Stats:
    endpoint = url.rstrip("/") + "/v1/events"
    clock = StepClock(datetime.now(UTC), timedelta(seconds=1 / tps))
    stream = generate(load_config(), seed, clock)
    stats = Stats()
    lock = asyncio.Lock()
    bucket = TokenBucket(tps)
    tasks: list[asyncio.Task[None]] = []
    stop = asyncio.Event()
    reporter = asyncio.create_task(_report(stats, stop, lock)) if report else None
    started = time.monotonic()
    deadline = started + duration_s
    try:
        while time.monotonic() < deadline:
            await bucket.acquire()
            if time.monotonic() >= deadline:
                break
            item = next(stream)
            async with lock:
                stats.generated += 1
            tasks.append(
                asyncio.create_task(post_payment(client, endpoint, item.payment, stats, lock))
            )
        # Rate is admissions during the run, not how long the last responses take.
        stats.elapsed_s = time.monotonic() - started
        await asyncio.gather(*tasks)
    finally:
        if stats.elapsed_s == 0:
            stats.elapsed_s = time.monotonic() - started
        stop.set()
        if reporter is not None:
            await reporter
    return stats


async def _report(stats: Stats, stop: asyncio.Event, lock: asyncio.Lock) -> None:
    while True:
        try:
            await asyncio.wait_for(stop.wait(), timeout=_REPORT_INTERVAL_S)
        except TimeoutError:
            async with lock:
                print(_summary(stats), file=sys.stderr)
            continue
        return


def _summary(stats: Stats) -> str:
    snapshot = stats.to_json()
    return (
        "generated={generated} sent={sent} accepted={accepted} "
        "duplicates={duplicates} http_422={http_422} http_503={http_503} "
        "retries={retries} failed={failed} achieved_tps={achieved_tps:.1f} "
        "p50_ms={latency_p50_ms:.1f} p95_ms={latency_p95_ms:.1f}"
    ).format(**snapshot)


def _retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        return None


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = math.ceil(percentile / 100 * len(ordered)) - 1
    index = min(max(rank, 0), len(ordered) - 1)
    return ordered[index]
