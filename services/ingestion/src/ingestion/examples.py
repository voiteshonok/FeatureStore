"""Request and response examples for payment intake.

The test PAN is the canonical Visa test number. It is accepted only here.
"""

from __future__ import annotations

from typing import Any

EVENT_ID = "6a4e2f0c-1d8b-4a37-9b9e-2c7f5e1a0b44"
EDGE_EVENT_ID = "7b5f3a1d-2e9c-4b48-8c0f-3d8a6f2b1c55"
TEST_PAN = "4111111111111111"
TS = "2026-11-24T18:00:00.000Z"

PAYMENT_TYPICAL: dict[str, Any] = {
    "event_id": EVENT_ID,
    "payment_id": "pay-2026-11-24-0001",
    "pan": TEST_PAN,
    "merchant_id": "m7",
    "amount_minor": 2599,
    "currency": "EUR",
    "status": "approved",
    "event_time": TS,
    "lat": 54.6872,
    "lon": 25.2797,
}
PAYMENT_EDGE: dict[str, Any] = {
    "event_id": EDGE_EVENT_ID,
    "payment_id": "pay-edge-1",
    "pan": TEST_PAN,
    "merchant_id": "m3",
    "amount_minor": 1,
    "currency": "EUR",
    "status": "declined",
    "event_time": "2026-11-24T18:01:12.418Z",
}

PAYMENT_EXAMPLES: dict[str, dict[str, Any]] = {
    "typical": {"summary": "Approved card payment with the test PAN", "value": PAYMENT_TYPICAL},
    "edge": {"summary": "Declined payment, minimum amount, no coordinates", "value": PAYMENT_EDGE},
}

ACCEPTED_EXAMPLE: dict[str, Any] = {"event_id": EVENT_ID, "duplicate": False}
DUPLICATE_EXAMPLE: dict[str, Any] = {"event_id": EVENT_ID, "duplicate": True}

HEALTH_EXAMPLE: dict[str, str] = {"status": "ok"}
READY_OK: dict[str, str] = {"status": "ready", "kafka": "ok", "postgres": "ok"}
READY_DOWN: dict[str, str] = {"status": "not_ready", "kafka": "down", "postgres": "ok"}

API_DESCRIPTION = (
    "Internal payment intake. The PAN is tokenized before the event is published. "
    "This service is not part of the public gateway API."
)

OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "events", "description": "Payment intake. Idempotent by event_id."},
    {"name": "ops", "description": "Process health and readiness probes."},
]

EVENT_DESCRIPTION = (
    "Accept a payment. The PAN is tokenized in this process and is not stored. "
    "Sending the same event_id again returns duplicate true and does not publish."
)
HEALTH_DESCRIPTION = "The process is up. This probe does not check Kafka or Postgres."
READY_DESCRIPTION = "Kafka and Postgres each answer within one second."

ERROR_CODES: dict[tuple[str, str], tuple[str, ...]] = {
    ("get", "/health"): ("500",),
    ("get", "/ready"): ("500",),
    ("post", "/v1/events"): ("422", "503", "500"),
}
