"""Request and response examples for the features HTTP API."""

from __future__ import annotations

from typing import Any

from schemas.openapi_problems import set_json_example

SAMPLE_CARD = "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00"
TS = "2026-11-24T18:00:00.000Z"
REQUEST_ID = "3b7f0d0e-5c9a-4c27-a1b1-f0f3d2a9e812"

FEATURE_TYPICAL: dict[str, Any] = {
    "card_token": SAMPLE_CARD,
    "merchant_id": "m7",
    "ts": TS,
    "lat": 54.6872,
    "lon": 25.2797,
}
FEATURE_EDGE: dict[str, Any] = {"merchant_id": "m7", "ts": TS}

FEATURE_EXAMPLES: dict[str, dict[str, Any]] = {
    "typical": {"summary": "Card and merchant", "value": FEATURE_TYPICAL},
    "edge": {"summary": "Merchant only", "value": FEATURE_EDGE},
}

FEATURE_RESPONSE_EXAMPLE: dict[str, Any] = {
    "request_id": REQUEST_ID,
    "features": {
        "card_cnt_1m": {
            "value": 4,
            "as_of": "2026-11-24T17:59:58.000Z",
            "age_ms": 2140,
            "status": "fresh",
        },
        "merchant_cnt_24h": {
            "value": 1312,
            "as_of": "2026-11-24T17:59:59.000Z",
            "age_ms": 1020,
            "status": "fresh",
        },
        "geo_velocity_kmh": {"value": 0.0, "as_of": None, "age_ms": None, "status": "fresh"},
    },
}

HEALTH_EXAMPLE: dict[str, str] = {"status": "ok"}
READY_OK: dict[str, str] = {"status": "ready", "redis": "ok", "postgres": "ok"}
READY_DOWN: dict[str, str] = {"status": "not_ready", "redis": "down", "postgres": "ok"}

API_DESCRIPTION = (
    "Internal feature lookup. Scoring clients reach this API through the gateway, "
    "not directly. POST keeps the card token and coordinates out of URLs and logs."
)

OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "features", "description": "Feature lookup by card token, merchant id, or both."},
    {"name": "ops", "description": "Process health and readiness probes."},
]

FEATURES_DESCRIPTION = (
    "Read the feature vector for a card, a merchant, or both. "
    "POST is used because the body carries a card token and coordinates: "
    "they must not appear in URLs, access logs, or proxy caches."
)
HEALTH_DESCRIPTION = "The process is up. This probe does not check Redis or Postgres."
READY_DESCRIPTION = "Redis and Postgres each answer within one second."


def apply_response_examples(schema: dict[str, Any]) -> None:
    set_json_example(schema, "get", "/health", "200", HEALTH_EXAMPLE)
    set_json_example(schema, "get", "/ready", "200", READY_OK)
    set_json_example(schema, "get", "/ready", "503", READY_DOWN)
    set_json_example(schema, "post", "/v1/features", "200", FEATURE_RESPONSE_EXAMPLE)


ERROR_CODES: dict[tuple[str, str], tuple[str, ...]] = {
    ("get", "/health"): ("500",),
    ("get", "/ready"): ("500",),
    ("post", "/v1/features"): ("422", "503", "500"),
}
