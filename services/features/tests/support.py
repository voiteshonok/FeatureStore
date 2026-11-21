"""Shared event builder for feature worker tests."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from schemas import TransactionEvent

# Near wall clock so apply.lua absolute PEXPIREAT / HPEXPIREAT (ADR-017) stay in the future.
BASE = datetime.now(UTC).replace(microsecond=0)


def make_event(**overrides: object) -> TransactionEvent:
    payload: dict[str, object] = {
        "event_id": uuid4(),
        "payment_id": "pay-1",
        "merchant_id": "m-1",
        "amount_minor": 100,
        "currency": "EUR",
        "status": "approved",
        "event_time": BASE,
        "card_token": "card-1",
        "ingest_time": BASE,
        "lat": 52.5,
        "lon": 13.4,
    }
    payload.update(overrides)
    return TransactionEvent.model_validate(payload)


def at_ms(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, UTC)


def fixed_id(value: str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee") -> UUID:
    return UUID(value)
