"""Seeded stream of synthetic payments and their send instructions."""

import hashlib
import random
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from schemas import PaymentIn
from schemas.time import to_utc_milliseconds
from simulator.config import SimulatorConfig
from simulator.population import (
    Merchant,
    bad_merchant_indexes,
    build_merchants,
    haversine_km,
    pan_for_index,
    sample_card,
)

Kind = Literal[
    "normal",
    "late_short",
    "late_long",
    "duplicate",
    "retry",
    "burst",
    "bad_merchant",
    "travel",
]
_SPEED_KMH = 900.0
_WHOLE_CITY_KM = 30.0


class Clock(Protocol):
    """Source of send time. Tests inject a clock that does not read the wall clock."""

    def now(self) -> datetime:
        """Return the next send time."""


class StepClock:
    """Advance a fixed start by ``step`` on every read."""

    def __init__(self, start: datetime, step: timedelta) -> None:
        self._at = start
        self._step = step

    def now(self) -> datetime:
        current = self._at
        self._at = current + self._step
        return current


def backfill_clock(
    t0: datetime,
    depth: timedelta,
    tps: float,
) -> tuple[StepClock, datetime, datetime]:
    """Clock, horizon and floor for a history of ``depth`` at ``tps``.

    The step is a whole number of microseconds, so a long run does not drift.
    The first tick is the floor. The horizon itself is not emitted.
    """
    if tps <= 0:
        msg = "tps must be positive"
        raise ValueError(msg)
    if depth <= timedelta(0):
        msg = "backfill depth must be positive"
        raise ValueError(msg)
    step_us = round(1_000_000 / tps)
    if step_us < 1:
        msg = "tps is too high for a microsecond step"
        raise ValueError(msg)
    horizon = _align(t0)
    floor = _align(horizon - depth)
    return StepClock(floor, timedelta(microseconds=step_us)), horizon, floor


@dataclass(frozen=True)
class GeneratedPayment:
    """One item the sender should transmit at ``send_at``."""

    payment: PaymentIn
    kind: Kind
    send_at: datetime


@dataclass
class AnomalyCounts:
    """How many anomaly payments were emitted by kind."""

    burst: int = 0
    bad_merchant: int = 0
    travel: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "burst": self.burst,
            "bad_merchant": self.bad_merchant,
            "travel": self.travel,
        }

    def add(self, kind: Kind) -> None:
        if kind == "burst":
            self.burst += 1
        elif kind == "bad_merchant":
            self.bad_merchant += 1
        elif kind == "travel":
            self.travel += 1


@dataclass
class GenerateState:
    """Mutable counters shared with the sender for the final JSON line."""

    anomalies: AnomalyCounts = field(default_factory=AnomalyCounts)


def generate(
    config: SimulatorConfig,
    seed: int,
    clock: Clock,
    *,
    run_id: str = "0",
    horizon: datetime | None = None,
    floor: datetime | None = None,
    state: GenerateState | None = None,
) -> Iterator[GeneratedPayment]:
    """Yield payments forever from a single ``random.Random(seed)``.

    Cards, merchants, amounts and timings depend on ``seed`` only.
    ``event_id`` and ``payment_id`` also depend on ``run_id``.

    ``horizon`` stops the stream when the next primary ``send_at`` reaches it.
    ``floor`` keeps a late ``event_time`` from falling earlier than the history
    start. Both are unset in the live mode, which keeps the open stream.

    When ``config.anomalies.enabled`` is false, the RNG sequence matches the
    pre-anomaly generator (no extra draws).
    """
    rng = random.Random(seed)  # noqa: S311
    merchants = build_merchants(config, rng)
    by_city: tuple[tuple[Merchant, ...], ...] = tuple(
        tuple(merchant for merchant in merchants if merchant.city_index == city)
        for city in range(len(config.cities))
    )
    bad_indexes = bad_merchant_indexes(config, merchants)
    last_normal: dict[int, tuple[datetime, float, float, Merchant]] = {}
    payment_seq = 0
    counters = state.anomalies if state is not None else AnomalyCounts()
    step_s = 1.0 / config.tps
    anomalies = config.anomalies

    while True:
        card = sample_card(
            rng,
            config.n_cards,
            tail_shape=config.tail_shape,
            max_weight_ratio=config.max_weight_ratio,
        )
        send_at = _align(clock.now())
        if horizon is not None and send_at >= horizon:
            return

        if (
            anomalies.enabled
            and anomalies.burst.enabled
            and _roll_per_min(rng, anomalies.burst.rate_per_min, step_s)
        ):
            payment_seq, burst_items = _make_burst(
                config,
                rng,
                by_city,
                seed=seed,
                run_id=run_id,
                payment_seq=payment_seq,
                anchor=send_at,
                last_normal=last_normal,
            )
            for item in burst_items:
                counters.add(item.kind)
                yield item
            continue

        if (
            anomalies.enabled
            and anomalies.travel.enabled
            and _roll_per_min(rng, anomalies.travel.rate_per_min, step_s)
        ):
            payment_seq, travel_items = _make_travel(
                config,
                rng,
                by_city,
                seed=seed,
                run_id=run_id,
                payment_seq=payment_seq,
                anchor=send_at,
            )
            for item in travel_items:
                counters.add(item.kind)
                yield item
            continue

        use_bad = bool(
            anomalies.enabled
            and anomalies.bad_merchant.enabled
            and bad_indexes
            and rng.random() < anomalies.bad_merchant.traffic_share
        )
        if use_bad:
            merchant = merchants[rng.choice(bad_indexes)]
            lat, lon = merchant.lat, merchant.lon
            kind: Kind = "bad_merchant"
            event_time = send_at
        else:
            merchant, lat, lon = _place(config, rng, by_city, card, send_at, last_normal)
            kind, event_time = _timing(config, rng, send_at, floor)

        payment_seq += 1
        status: Literal["approved", "declined"] = (
            "declined" if rng.random() < merchant.decline_rate else "approved"
        )
        amount = max(1, int(rng.lognormvariate(config.amount_log_mean, config.amount_log_sigma)))
        payment_id = _payment_id(seed, run_id, payment_seq)
        payment = _payment(
            config,
            card=card,
            merchant=merchant,
            lat=lat,
            lon=lon,
            payment_id=payment_id,
            event_id=_event_id(seed, run_id, payment_seq, 0),
            amount=amount,
            status=status,
            event_time=event_time,
        )
        if kind in {"normal", "bad_merchant"}:
            last_normal[card] = (payment.event_time, lat, lon, merchant)
        yield GeneratedPayment(payment=payment, kind=kind, send_at=send_at)
        if kind == "bad_merchant":
            counters.add("bad_merchant")

        if kind == "bad_merchant":
            continue
        if rng.random() < config.duplicate_rate:
            duplicate_at = _align(clock.now()) if horizon is None else send_at
            yield GeneratedPayment(payment=payment, kind="duplicate", send_at=duplicate_at)
        if status == "declined" and rng.random() < config.retry_rate:
            retry_at = _align(clock.now()) if horizon is None else send_at
            retry_status: Literal["approved", "declined"] = (
                "declined" if rng.random() < merchant.decline_rate else "approved"
            )
            retry = _payment(
                config,
                card=card,
                merchant=merchant,
                lat=lat,
                lon=lon,
                payment_id=payment.payment_id,
                event_id=_event_id(seed, run_id, payment_seq, 1),
                amount=amount,
                status=retry_status,
                event_time=retry_at,
            )
            yield GeneratedPayment(payment=retry, kind="retry", send_at=retry_at)


def generate_burst(
    config: SimulatorConfig,
    *,
    seed: int,
    card_index: int,
    count: int,
    run_id: str = "0",
    now: datetime | None = None,
) -> list[GeneratedPayment]:
    """Build ``count`` same-card payments for the one-shot ``simulator burst`` CLI."""
    if count < 1:
        msg = "burst count must be positive"
        raise ValueError(msg)
    if card_index < 0 or card_index >= config.n_cards:
        msg = "card index out of range"
        raise ValueError(msg)
    rng = random.Random(seed)  # noqa: S311
    merchants = build_merchants(config, rng)
    by_city: tuple[tuple[Merchant, ...], ...] = tuple(
        tuple(merchant for merchant in merchants if merchant.city_index == city)
        for city in range(len(config.cities))
    )
    city = by_city[card_index % len(config.cities)]
    anchor = _align(now if now is not None else datetime.now(UTC))
    window = config.anomalies.burst.window_s
    items: list[GeneratedPayment] = []
    # Spread event_times in the past ending at ``anchor`` (ingestion rejects >5s future).
    span = min(window * 0.9, 30.0)
    for offset in range(count):
        merchant = rng.choice(city)
        frac = 0.0 if count == 1 else offset / (count - 1)
        event_time = _align(anchor - timedelta(seconds=(1.0 - frac) * span))
        amount = max(1, int(rng.lognormvariate(config.amount_log_mean, config.amount_log_sigma)))
        status: Literal["approved", "declined"] = (
            "declined" if rng.random() < merchant.decline_rate else "approved"
        )
        seq = offset + 1
        payment = _payment(
            config,
            card=card_index,
            merchant=merchant,
            lat=merchant.lat,
            lon=merchant.lon,
            payment_id=_payment_id(seed, run_id, seq),
            event_id=_event_id(seed, run_id, seq, 0),
            amount=amount,
            status=status,
            event_time=event_time,
        )
        items.append(GeneratedPayment(payment=payment, kind="burst", send_at=anchor))
    return items


def _roll_per_min(rng: random.Random, rate_per_min: float, step_s: float) -> bool:
    """Bernoulli trial so expected triggers per minute ≈ ``rate_per_min``."""
    p = min(1.0, rate_per_min * step_s / 60.0)
    return rng.random() < p


def _make_burst(
    config: SimulatorConfig,
    rng: random.Random,
    by_city: tuple[tuple[Merchant, ...], ...],
    *,
    seed: int,
    run_id: str,
    payment_seq: int,
    anchor: datetime,
    last_normal: dict[int, tuple[datetime, float, float, Merchant]],
) -> tuple[int, list[GeneratedPayment]]:
    burst = config.anomalies.burst
    card = sample_card(
        rng,
        config.n_cards,
        tail_shape=config.tail_shape,
        max_weight_ratio=config.max_weight_ratio,
    )
    size = rng.randint(burst.size_min, burst.size_max)
    city = by_city[card % len(config.cities)]
    seq = payment_seq
    items: list[GeneratedPayment] = []
    span = burst.window_s * 0.9
    for index in range(size):
        seq += 1
        merchant = rng.choice(city)
        frac = 0.0 if size == 1 else index / (size - 1)
        # Past window ending at ``anchor`` so ingestion accepts event_time.
        event_time = _align(anchor - timedelta(seconds=(1.0 - frac) * span))
        amount = max(1, int(rng.lognormvariate(config.amount_log_mean, config.amount_log_sigma)))
        status: Literal["approved", "declined"] = (
            "declined" if rng.random() < merchant.decline_rate else "approved"
        )
        payment = _payment(
            config,
            card=card,
            merchant=merchant,
            lat=merchant.lat,
            lon=merchant.lon,
            payment_id=_payment_id(seed, run_id, seq),
            event_id=_event_id(seed, run_id, seq, 0),
            amount=amount,
            status=status,
            event_time=event_time,
        )
        items.append(GeneratedPayment(payment=payment, kind="burst", send_at=anchor))
        last_normal[card] = (event_time, merchant.lat, merchant.lon, merchant)
    return seq, items


def _make_travel(
    config: SimulatorConfig,
    rng: random.Random,
    by_city: tuple[tuple[Merchant, ...], ...],
    *,
    seed: int,
    run_id: str,
    payment_seq: int,
    anchor: datetime,
) -> tuple[int, list[GeneratedPayment]]:
    travel = config.anomalies.travel
    if len(config.cities) < 2:
        return payment_seq, []
    card = sample_card(
        rng,
        config.n_cards,
        tail_shape=config.tail_shape,
        max_weight_ratio=config.max_weight_ratio,
    )
    city_a = card % len(config.cities)
    city_b = (city_a + 1 + rng.randrange(len(config.cities) - 1)) % len(config.cities)
    first = rng.choice(by_city[city_a])
    second = rng.choice(by_city[city_b])
    gap_s = rng.uniform(travel.gap_min_s, travel.gap_max_s)
    # Both event_times ≤ anchor (ingestion allows at most +5s future).
    first_at = _align(anchor - timedelta(seconds=gap_s))
    second_at = anchor
    seq = payment_seq
    items: list[GeneratedPayment] = []
    for merchant, event_time in ((first, first_at), (second, second_at)):
        seq += 1
        amount = max(1, int(rng.lognormvariate(config.amount_log_mean, config.amount_log_sigma)))
        status: Literal["approved", "declined"] = (
            "declined" if rng.random() < merchant.decline_rate else "approved"
        )
        payment = _payment(
            config,
            card=card,
            merchant=merchant,
            lat=merchant.lat,
            lon=merchant.lon,
            payment_id=_payment_id(seed, run_id, seq),
            event_id=_event_id(seed, run_id, seq, 0),
            amount=amount,
            status=status,
            event_time=event_time,
        )
        items.append(GeneratedPayment(payment=payment, kind="travel", send_at=anchor))
    return seq, items


def _place(
    config: SimulatorConfig,
    rng: random.Random,
    by_city: tuple[tuple[Merchant, ...], ...],
    card: int,
    send_at: datetime,
    last_normal: dict[int, tuple[datetime, float, float, Merchant]],
) -> tuple[Merchant, float, float]:
    city = by_city[card % len(config.cities)]
    anchor = last_normal.get(card)
    if anchor is None:
        merchant = rng.choice(city)
        return merchant, merchant.lat, merchant.lon
    previous_at, previous_lat, previous_lon, previous = anchor
    elapsed_h = (send_at - previous_at).total_seconds() / 3600
    if elapsed_h <= 0:
        return previous, previous_lat, previous_lon
    max_km = _SPEED_KMH * elapsed_h
    if max_km >= _WHOLE_CITY_KM:
        merchant = rng.choice(city)
        return merchant, merchant.lat, merchant.lon
    if max_km < 0.05:
        return previous, previous_lat, previous_lon
    # A few draws stay inside the speed cap. If none do, keep the last point.
    for _ in range(16):
        merchant = rng.choice(city)
        if haversine_km(previous_lat, previous_lon, merchant.lat, merchant.lon) <= max_km:
            return merchant, merchant.lat, merchant.lon
    return previous, previous_lat, previous_lon


def _timing(
    config: SimulatorConfig,
    rng: random.Random,
    send_at: datetime,
    floor: datetime | None = None,
) -> tuple[Kind, datetime]:
    roll = rng.random()
    if roll < config.late_long_rate:
        offset = rng.uniform(config.late_long_min_s, config.late_long_max_s)
        return "late_long", _not_before(send_at - timedelta(seconds=offset), floor)
    if roll < config.late_long_rate + config.late_short_rate:
        offset = rng.uniform(config.late_short_min_s, config.late_short_max_s)
        return "late_short", _not_before(send_at - timedelta(seconds=offset), floor)
    return "normal", send_at


def _payment(
    config: SimulatorConfig,
    *,
    card: int,
    merchant: Merchant,
    lat: float,
    lon: float,
    payment_id: str,
    event_id: UUID,
    amount: int,
    status: Literal["approved", "declined"],
    event_time: datetime,
) -> PaymentIn:
    return PaymentIn(
        event_id=event_id,
        payment_id=payment_id,
        pan=pan_for_index(card, bin=config.bin, length=config.pan_length),
        merchant_id=merchant.merchant_id,
        amount_minor=amount,
        currency="EUR",
        status=status,
        event_time=event_time,
        lat=lat,
        lon=lon,
    )


def _event_id(seed: int, run_id: str, seq: int, delivery: int) -> UUID:
    return UUID(bytes=_digest("event", seed, run_id, seq, delivery)[:16])


def _payment_id(seed: int, run_id: str, seq: int) -> str:
    return str(UUID(bytes=_digest("payment", seed, run_id, seq)[:16]))


def _digest(*parts: object) -> bytes:
    payload = "\0".join(str(part) for part in parts).encode()
    return hashlib.sha256(payload).digest()


def _align(value: datetime) -> datetime:
    return to_utc_milliseconds(value)


def _not_before(value: datetime, floor: datetime | None) -> datetime:
    event_time = _align(value)
    if floor is not None and event_time < floor:
        return _align(floor)
    return event_time
