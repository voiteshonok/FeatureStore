from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest

from schemas.pan import is_luhn
from simulator import GeneratedPayment, StepClock, card_home, generate, load_config, pan_for_index
from simulator.population import haversine_km

START = datetime(2026, 1, 1, tzinfo=UTC)
STEP = timedelta(milliseconds=5)
SEED = 0
_PRIMARY = frozenset({"normal", "late_short", "late_long"})


def _clock() -> StepClock:
    return StepClock(START, STEP)


def _take(seed: int, count: int) -> list[GeneratedPayment]:
    return list(islice(generate(load_config(), seed, _clock()), count))


@pytest.fixture(scope="module")
def stream() -> list[GeneratedPayment]:
    return _take(SEED, 100_000)


def test_same_seed_replays_the_first_10_000() -> None:
    assert _take(7, 10_000) == _take(7, 10_000)
    assert _take(7, 100) != _take(8, 100)


def test_pan_is_luhn_and_stable() -> None:
    for index in (0, 1, 42, 2_999_999):
        pan = pan_for_index(index)
        assert pan == pan_for_index(index)
        assert len(pan) == 16
        assert is_luhn(pan)
    assert pan_for_index(0) != pan_for_index(1)


def test_card_home_stays_near_its_city() -> None:
    config = load_config()
    for index in (0, 1, 5, 2_999_999):
        assert card_home(index, config) == card_home(index, config)
        city = config.cities[index % len(config.cities)]
        assert haversine_km(city.lat, city.lon, *card_home(index, config)) <= 3


def test_profile_rates() -> None:
    config = load_config()
    assert config.n_cards == 3_000_000
    assert config.n_merchants == 5_000
    assert config.duplicate_rate == 0.01
    assert config.late_short_rate == 0.02
    assert config.late_long_rate == 0.001
    assert config.retry_rate == 0.3
    assert config.tps == 200


def test_duplicate_late_and_retry_shares(stream: list[GeneratedPayment]) -> None:
    config = load_config()
    primary = [item for item in stream if item.kind in _PRIMARY]
    declined = [item for item in primary if item.payment.status == "declined"]
    assert primary
    assert declined
    _within(_count(stream, "duplicate") / len(primary), config.duplicate_rate)
    _within(_count(stream, "late_short") / len(primary), config.late_short_rate)
    _within(_count(stream, "late_long") / len(primary), config.late_long_rate)
    _within(_count(stream, "retry") / len(declined), config.retry_rate)


def test_late_events_stay_inside_24h(stream: list[GeneratedPayment]) -> None:
    window = timedelta(hours=24)
    for item in stream:
        if item.kind not in {"late_short", "late_long"}:
            continue
        age = item.send_at - item.payment.event_time
        assert timedelta(0) < age < window


def test_normal_traffic_stays_under_900_kmh(stream: list[GeneratedPayment]) -> None:
    by_pan: dict[str, list[GeneratedPayment]] = defaultdict(list)
    for item in stream:
        if item.kind == "normal":
            by_pan[item.payment.pan].append(item)
    for events in by_pan.values():
        events.sort(key=lambda item: item.payment.event_time)
        for earlier, later in zip(events, events[1:], strict=False):
            assert earlier.payment.lat is not None
            assert earlier.payment.lon is not None
            assert later.payment.lat is not None
            assert later.payment.lon is not None
            distance = haversine_km(
                earlier.payment.lat,
                earlier.payment.lon,
                later.payment.lat,
                later.payment.lon,
            )
            elapsed = (later.payment.event_time - earlier.payment.event_time).total_seconds()
            if elapsed == 0:
                assert distance == 0
                continue
            assert distance / (elapsed / 3600) <= 900


def test_duplicates_repeat_the_payment(stream: list[GeneratedPayment]) -> None:
    for previous, item in zip(stream, stream[1:], strict=False):
        if item.kind != "duplicate":
            continue
        assert previous.payment == item.payment
        assert previous.kind in _PRIMARY
        assert item.send_at > previous.send_at


def _count(stream: list[GeneratedPayment], kind: str) -> int:
    return sum(1 for item in stream if item.kind == kind)


def _within(observed: float, expected: float) -> None:
    assert abs(observed - expected) <= 0.2 * expected, (observed, expected)
