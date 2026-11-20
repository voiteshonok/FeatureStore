from datetime import UTC, datetime, timedelta

from baseline.windows import (
    MINUTE_MS,
    in_card_window,
    in_merchant_window,
    merchant_bounds,
    to_epoch_ms,
)
from schemas import for_entity

_T = datetime(2026, 6, 1, 12, 0, 30, tzinfo=UTC)


def test_event_on_the_lower_bound_is_outside_and_t_is_inside() -> None:
    for spec in for_entity("card"):
        if spec.window is None:
            continue
        assert not in_card_window(_T - spec.window, _T, spec.window)
        assert in_card_window(_T, _T, spec.window)
        assert not in_card_window(_T + timedelta(milliseconds=1), _T, spec.window)
        assert in_card_window(_T - spec.window + timedelta(milliseconds=1), _T, spec.window)


def test_merchant_bucket_1440_back_is_outside_and_1439_is_inside() -> None:
    start, end = merchant_bounds(_T)
    current = to_epoch_ms(_T) // MINUTE_MS
    assert to_epoch_ms(start) // MINUTE_MS == current - 1_439
    assert to_epoch_ms(start - timedelta(minutes=1)) // MINUTE_MS == current - 1_440
    assert in_merchant_window(start, _T)
    assert in_merchant_window(_T, _T)
    assert in_merchant_window(end - timedelta(milliseconds=1), _T)
    assert not in_merchant_window(end, _T)
    assert not in_merchant_window(start - timedelta(minutes=1), _T)
