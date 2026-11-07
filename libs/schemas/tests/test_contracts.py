from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from schemas import (
    FeatureRequest,
    FeatureResponse,
    FeatureStatus,
    FeatureValue,
    PaymentIn,
    TransactionEvent,
    check_event_time,
)
from schemas.pan import is_luhn

VALID_PAN = "4111111111111111"
EVENT_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
NOW = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)


def _pan_with_luhn(body: str) -> str:
    for digit in "0123456789":
        candidate = body + digit
        if is_luhn(candidate):
            return candidate
    raise AssertionError("no Luhn check digit")


def payment_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_id": EVENT_ID,
        "payment_id": "pay-1",
        "pan": VALID_PAN,
        "merchant_id": "m-1",
        "amount_minor": 199,
        "currency": "EUR",
        "status": "approved",
        "event_time": NOW,
    }
    payload.update(overrides)
    return payload


def event_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "event_id": EVENT_ID,
        "payment_id": "pay-1",
        "merchant_id": "m-1",
        "amount_minor": 199,
        "currency": "EUR",
        "status": "approved",
        "event_time": NOW,
        "card_token": "ab" * 32,
        "ingest_time": NOW,
    }
    payload.update(overrides)
    return payload


def error_fields(exc: ValidationError) -> set[str]:
    return {str(err["loc"][0]) for err in exc.errors() if err["loc"]}


def test_valid_payment_in() -> None:
    payment = PaymentIn.model_validate(payment_payload(lat=50.0, lon=10.0))
    assert payment.pan == VALID_PAN
    assert payment.amount_minor == 199
    assert payment.lat == 50.0
    assert payment.lon == 10.0


def test_valid_payment_in_without_coords() -> None:
    payment = PaymentIn.model_validate(payment_payload())
    assert payment.lat is None
    assert payment.lon is None


@pytest.mark.parametrize("length", [13, 19])
def test_valid_pan_length_boundaries(length: int) -> None:
    pan = _pan_with_luhn("4" * (length - 1))
    payment = PaymentIn.model_validate(payment_payload(pan=pan))
    assert payment.pan == pan


@pytest.mark.parametrize(
    ("overrides", "field"),
    [
        ({"pan": "411111111111"}, "pan"),
        ({"pan": "41111111111111111111"}, "pan"),
        ({"pan": "4111-1111-1111-1111"}, "pan"),
        ({"pan": "4111111111111112"}, "pan"),
        ({"amount_minor": 0}, "amount_minor"),
        ({"amount_minor": -1}, "amount_minor"),
        ({"currency": "USD"}, "currency"),
        ({"status": "pending"}, "status"),
        ({"lat": 90.1, "lon": 0.0}, "lat"),
        ({"lat": 0.0, "lon": 180.1}, "lon"),
        ({"lat": 1.0}, "lon"),
        ({"lon": 1.0}, "lat"),
        ({"event_time": datetime(2024, 6, 15, 12, 0, 0)}, "event_time"),
    ],
)
def test_invalid_payment_in_fails_on_field(overrides: dict[str, Any], field: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        PaymentIn.model_validate(payment_payload(**overrides))
    assert field in error_fields(exc_info.value)


def test_event_time_non_utc_converted_and_truncated_to_ms() -> None:
    offset = timezone(timedelta(hours=3))
    raw = datetime(2024, 6, 15, 15, 0, 0, 123456, tzinfo=offset)
    payment = PaymentIn.model_validate(payment_payload(event_time=raw))
    assert payment.event_time.tzinfo is UTC
    assert payment.event_time == datetime(2024, 6, 15, 12, 0, 0, 123000, tzinfo=UTC)


def test_transaction_event_has_no_pan_field() -> None:
    assert "pan" not in TransactionEvent.model_fields


def test_valid_transaction_event() -> None:
    event = TransactionEvent.model_validate(event_payload())
    assert event.schema_version == 1
    assert event.card_token == "ab" * 32


def test_unknown_field_rejected_on_transaction_event() -> None:
    with pytest.raises(ValidationError) as exc_info:
        TransactionEvent.model_validate(event_payload(pan=VALID_PAN))
    assert "pan" in error_fields(exc_info.value)


def test_transaction_event_json_round_trip() -> None:
    event = TransactionEvent.model_validate(event_payload(lat=1.5, lon=2.5))
    restored = TransactionEvent.from_bytes(event.to_bytes())
    assert restored == event


def test_transaction_event_is_frozen() -> None:
    event = TransactionEvent.model_validate(event_payload())
    with pytest.raises(ValidationError):
        event.amount_minor = 1  # type: ignore[misc]


def test_feature_models_valid() -> None:
    value = FeatureValue(value=1.5, as_of=NOW, age_ms=10, status=FeatureStatus.fresh)
    request = FeatureRequest.model_validate(
        {
            "card_token": "tok",
            "merchant_id": "m-1",
            "lat": 10.0,
            "lon": 20.0,
            "ts": NOW,
        }
    )
    response = FeatureResponse(request_id=uuid4(), features={"amount_avg": value})
    assert request.merchant_id == "m-1"
    assert response.features["amount_avg"].status is FeatureStatus.fresh


def test_check_event_time_boundaries() -> None:
    assert check_event_time(NOW + timedelta(seconds=5), NOW) is None
    assert check_event_time(NOW - timedelta(hours=24), NOW) is None
    assert check_event_time(NOW + timedelta(seconds=5, milliseconds=1), NOW) is not None
    assert check_event_time(NOW - timedelta(hours=24, milliseconds=1), NOW) is not None


def test_check_event_time_inside_window() -> None:
    assert check_event_time(NOW, NOW) is None
    assert check_event_time(NOW - timedelta(hours=1), NOW) is None


def test_check_event_time_rejects_naive() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        check_event_time(datetime(2024, 6, 15, 12, 0, 0), NOW)


def test_luhn_handles_doubled_digit_above_nine() -> None:
    assert is_luhn("79927398713") is True
