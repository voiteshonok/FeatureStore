"""Seeded stream of synthetic payments and their send instructions."""

import hashlib
import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from schemas import PaymentIn
from schemas.time import to_utc_milliseconds
from simulator.config import SimulatorConfig
from simulator.population import Merchant, build_merchants, haversine_km, pan_for_index, sample_card

Kind = Literal["normal", "late_short", "late_long", "duplicate", "retry"]
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


def generate(
    config: SimulatorConfig,
    seed: int,
    clock: Clock,
    *,
    run_id: str = "0",
    horizon: datetime | None = None,
    floor: datetime | None = None,
) -> Iterator[GeneratedPayment]:
    """Yield payments forever from a single ``random.Random(seed)``.

    Cards, merchants, amounts and timings depend on ``seed`` only.
    ``event_id`` and ``payment_id`` also depend on ``run_id``.

    ``horizon`` stops the stream when the next primary ``send_at`` reaches it.
    ``floor`` keeps a late ``event_time`` from falling earlier than the history
    start. Both are unset in the live mode, which keeps the open stream.
    """
    rng = random.Random(seed)  # noqa: S311
    merchants = build_merchants(config, rng)
    by_city: tuple[tuple[Merchant, ...], ...] = tuple(
        tuple(merchant for merchant in merchants if merchant.city_index == city)
        for city in range(len(config.cities))
    )
    last_normal: dict[int, tuple[datetime, float, float, Merchant]] = {}
    payment_seq = 0
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
        if kind == "normal":
            last_normal[card] = (payment.event_time, lat, lon, merchant)
        yield GeneratedPayment(payment=payment, kind=kind, send_at=send_at)
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
