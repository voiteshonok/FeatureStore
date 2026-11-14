"""Seeded stream of synthetic payments and their send instructions."""

import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from schemas import PaymentIn
from schemas.time import to_utc_milliseconds
from simulator.config import SimulatorConfig
from simulator.population import Merchant, build_merchants, haversine_km, pan_for_index, sample_zipf

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
) -> Iterator[GeneratedPayment]:
    """Yield payments forever from a single ``random.Random(seed)``."""
    rng = random.Random(seed)  # noqa: S311
    merchants = build_merchants(config, rng)
    by_city: tuple[tuple[Merchant, ...], ...] = tuple(
        tuple(merchant for merchant in merchants if merchant.city_index == city)
        for city in range(len(config.cities))
    )
    last_normal: dict[int, tuple[datetime, float, float, Merchant]] = {}
    payment_seq = 0
    while True:
        card = sample_zipf(rng, config.zipf_alpha, config.n_cards) - 1
        send_at = _align(clock.now())
        merchant, lat, lon = _place(config, rng, by_city, card, send_at, last_normal)
        kind, event_time = _timing(config, rng, send_at)
        payment_seq += 1
        status: Literal["approved", "declined"] = (
            "declined" if rng.random() < merchant.decline_rate else "approved"
        )
        amount = max(1, int(rng.lognormvariate(config.amount_log_mean, config.amount_log_sigma)))
        payment = _payment(
            config,
            card=card,
            merchant=merchant,
            lat=lat,
            lon=lon,
            payment_id=str(payment_seq),
            event_id=_event_id(rng),
            amount=amount,
            status=status,
            event_time=event_time,
        )
        if kind == "normal":
            last_normal[card] = (payment.event_time, lat, lon, merchant)
        yield GeneratedPayment(payment=payment, kind=kind, send_at=send_at)
        if rng.random() < config.duplicate_rate:
            yield GeneratedPayment(payment=payment, kind="duplicate", send_at=_align(clock.now()))
        if status == "declined" and rng.random() < config.retry_rate:
            retry_at = _align(clock.now())
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
                event_id=_event_id(rng),
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
) -> tuple[Kind, datetime]:
    roll = rng.random()
    if roll < config.late_long_rate:
        offset = rng.uniform(config.late_long_min_s, config.late_long_max_s)
        return "late_long", _align(send_at - timedelta(seconds=offset))
    if roll < config.late_long_rate + config.late_short_rate:
        offset = rng.uniform(config.late_short_min_s, config.late_short_max_s)
        return "late_short", _align(send_at - timedelta(seconds=offset))
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


def _event_id(rng: random.Random) -> UUID:
    return UUID(int=rng.getrandbits(128))


def _align(value: datetime) -> datetime:
    return to_utc_milliseconds(value)
