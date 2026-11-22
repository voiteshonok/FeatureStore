"""Anomaly scenarios for the simulator (T7.6)."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from itertools import islice

import pytest

from simulator.cli import main
from simulator.config import load_config
from simulator.generator import StepClock, generate, generate_burst
from simulator.population import bad_merchant_indexes, build_merchants

START = datetime(2026, 1, 1, tzinfo=UTC)
STEP = timedelta(milliseconds=5)


def _clock() -> StepClock:
    return StepClock(START, STEP)


def _take(seed: int, count: int, *, anomalies: bool = False):
    config = load_config()
    if anomalies:
        config = config.with_anomalies_enabled()
    return list(islice(generate(config, seed, _clock()), count))


def test_without_anomalies_matches_baseline_stream() -> None:
    """No --anomalies: stream matches the default YAML (enabled=false)."""
    plain = list(islice(generate(load_config(), 7, _clock()), 5_000))
    assert plain == _take(7, 5_000, anomalies=False)
    assert [item.kind for item in plain if item.kind in {"burst", "bad_merchant", "travel"}] == []


def test_anomaly_seed_replays() -> None:
    first = _take(11, 2_000, anomalies=True)
    second = _take(11, 2_000, anomalies=True)
    assert first == second
    assert [item.kind for item in first] == [item.kind for item in second]
    assert [item.kind for item in first] != [item.kind for item in _take(12, 2_000, anomalies=True)]


def test_burst_puts_six_plus_events_on_one_card_in_60s() -> None:
    items = _take(21, 20_000, anomalies=True)
    bursts = [item for item in items if item.kind == "burst"]
    assert bursts, "expected at least one burst in 20k events"
    by_pan: dict[str, list[datetime]] = defaultdict(list)
    for item in bursts:
        by_pan[item.payment.pan].append(item.payment.event_time)
    found = False
    for times in by_pan.values():
        times.sort()
        for index, start in enumerate(times):
            end = start + timedelta(seconds=60)
            window = sum(1 for stamp in times[index:] if stamp <= end)
            if window >= 6:
                found = True
                break
        if found:
            break
    assert found, "no card had ≥6 burst events inside 60s"


def test_bad_merchant_decline_rate_above_threshold() -> None:
    config = load_config().with_anomalies_enabled()
    items = list(islice(generate(config, 3, _clock()), 50_000))
    bad = [item for item in items if item.kind == "bad_merchant"]
    assert len(bad) >= 100, "expected bad_merchant traffic"
    by_merchant: dict[str, Counter[str]] = defaultdict(Counter)
    for item in bad:
        by_merchant[item.payment.merchant_id][item.payment.status] += 1
    assert any(
        counts["declined"] / sum(counts.values()) > 0.35
        for counts in by_merchant.values()
        if sum(counts.values()) >= 20
    )


def test_build_merchants_marks_bad_band() -> None:
    import random

    config = load_config().with_anomalies_enabled()
    rng = random.Random(0)  # noqa: S311
    merchants = build_merchants(config, rng)
    indexes = bad_merchant_indexes(config, merchants)
    assert len(indexes) == config.anomalies.bad_merchant.count
    for index in indexes:
        assert merchants[index].decline_rate >= config.anomalies.bad_merchant.decline_rate_min


def test_generate_burst_cli_payload() -> None:
    config = load_config()
    items = generate_burst(config, seed=1, card_index=0, count=12, run_id="demo")
    assert len(items) == 12
    assert all(item.kind == "burst" for item in items)
    assert len({item.payment.pan for item in items}) == 1
    times = sorted(item.payment.event_time for item in items)
    assert (times[-1] - times[0]) <= timedelta(seconds=60)


def test_cli_burst_requires_flags() -> None:
    with pytest.raises(SystemExit):
        main(["burst", "--count", "12", "--seed", "1", "--url", "http://x"])
    with pytest.raises(SystemExit):
        main(["burst", "--card-index", "0", "--seed", "1", "--url", "http://x"])


def test_cli_anomalies_flag_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    async def fake_sender(**kwargs: object) -> object:
        seen.update(kwargs)

        class _Stats:
            def to_json(self) -> dict[str, object]:
                return {"ok": 1, "anomalies": {"burst": 0, "bad_merchant": 0, "travel": 0}}

        return _Stats()

    monkeypatch.setattr("simulator.cli.run_sender", fake_sender)
    main(
        [
            "--tps",
            "10",
            "--duration",
            "1s",
            "--seed",
            "1",
            "--url",
            "http://example",
            "--run-id",
            "t",
            "--anomalies",
        ]
    )
    assert seen.get("anomalies") is True
