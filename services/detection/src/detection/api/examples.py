"""Request and response examples for the detection HTTP API."""

from __future__ import annotations

from typing import Any

from schemas.openapi_problems import set_json_example

ALERT_ID = "6a4e2f0c-1d8b-4a37-9b9e-2c7f5e1a0b44"
EVENT_ID = "3b7f0d0e-5c9a-4c27-a1b1-f0f3d2a9e811"
SAMPLE_CARD = "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00"
RULE_CARD_ID = "c0ffee00-0000-4000-8000-000000000001"
RULE_MERCHANT_ID = "c0ffee00-0000-4000-8000-000000000003"
OBSERVED_AT = "2026-11-24T18:01:12.418Z"
RESOLVE_NOTE = "Confirmed with cardholder, legitimate travel"

PATCH_ACKNOWLEDGE: dict[str, Any] = {"status": "acknowledged"}
PATCH_RESOLVE: dict[str, Any] = {"status": "resolved", "note": RESOLVE_NOTE}

PATCH_EXAMPLES: dict[str, dict[str, Any]] = {
    "typical": {"summary": "Acknowledge an open alert", "value": PATCH_ACKNOWLEDGE},
    "edge": {"summary": "Resolve with a note", "value": PATCH_RESOLVE},
}

ALERT_EXAMPLE: dict[str, Any] = {
    "alert_id": ALERT_ID,
    "rule_id": RULE_CARD_ID,
    "status": "open",
    "merchant_id": None,
    "card_token": SAMPLE_CARD,
    "event_id": EVENT_ID,
    "feature_snapshot": {"card_cnt_1m": 6},
    "created_at": OBSERVED_AT,
    "resolved_at": None,
    "acknowledged_at": None,
    "acknowledged_by": None,
    "resolved_by": None,
    "resolution_note": None,
}

ALERT_ACKNOWLEDGED: dict[str, Any] = {
    **ALERT_EXAMPLE,
    "status": "acknowledged",
    "acknowledged_at": "2026-11-24T18:05:00.000Z",
    "acknowledged_by": "analyst@featurestore.local",
}

RULE_CARD: dict[str, Any] = {
    "rule_id": RULE_CARD_ID,
    "name": "card_velocity_burst",
    "entity": "card",
    "enabled": True,
    "conditions": [{"feature_name": "card_cnt_1m", "comparison": "gt", "threshold": 5}],
}

RULE_MERCHANT: dict[str, Any] = {
    "rule_id": RULE_MERCHANT_ID,
    "name": "merchant_decline_wave",
    "entity": "merchant",
    "enabled": True,
    "conditions": [
        {"feature_name": "merchant_decline_rate_24h", "comparison": "gt", "threshold": 0.35},
        {"feature_name": "merchant_cnt_24h", "comparison": "ge", "threshold": 20},
    ],
}

AUDIT_EXAMPLE: dict[str, Any] = {
    "audit_id": 1,
    "occurred_at": OBSERVED_AT,
    "actor": "analyst@featurestore.local",
    "action": "created",
    "detail": {},
}

ALERT_LIST_EXAMPLE: dict[str, Any] = {"items": [ALERT_EXAMPLE], "next_cursor": None}
ALERT_DETAIL_EXAMPLE: dict[str, Any] = {
    "alert": ALERT_EXAMPLE,
    "rule": RULE_CARD,
    "history": [AUDIT_EXAMPLE],
}

HEALTH_EXAMPLE: dict[str, str] = {"status": "ok"}
READY_OK: dict[str, str] = {"status": "ready", "kafka": "ok", "postgres": "ok"}
READY_DOWN: dict[str, str] = {"status": "not_ready", "kafka": "down", "postgres": "ok"}

ALERT_SSE_EXAMPLE = (
    "event: alert\n"
    "id: 42\n"
    "data: "
    '{"id":42,"op":"created",'
    f'"alert_id":"{ALERT_ID}",'
    f'"rule_id":"{RULE_CARD_ID}",'
    '"rule_name":"card_velocity_burst","status":"open","entity":"card",'
    '"entity_id":"c0ff…e00","created_at":"2026-11-24T18:01:12.418Z"}\n'
    "\n"
)

API_DESCRIPTION = (
    "Internal alert and rule API. Operators reach it through the gateway, "
    "which sets X-Operator-Id and X-Operator-Role."
)

OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "alerts", "description": "Alert list, detail, and status transitions."},
    {"name": "rules", "description": "Alert rules stored as data."},
    {
        "name": "stream",
        "description": "Server-Sent Events. Payloads are in docs/asyncapi/alerts-stream.yaml.",
    },
    {"name": "ops", "description": "Process health and readiness probes."},
]

LIST_ALERTS_DESCRIPTION = "Keyset page of alerts. The gateway only proxies a session caller."
GET_ALERT_DESCRIPTION = "One alert with its rule and audit history."
PATCH_DESCRIPTION = (
    "Change an alert status. The gateway allows this only for roles analyst and admin "
    "and requires X-Requested-With: fetch. "
    "Allowed transitions: open to acknowledged, acknowledged to resolved, "
    "and open to resolved. resolved is final. "
    "A note of 3–500 characters is required when resolving."
)
RULES_DESCRIPTION = "Rules with their comparison conditions."
STREAM_DESCRIPTION = (
    "Server-Sent Events of alert and stats frames. Payloads: docs/asyncapi/alerts-stream.yaml."
)
HEALTH_DESCRIPTION = "The process is up. This probe does not check Kafka or Postgres."
READY_DESCRIPTION = "The consumer is running and Postgres answers within one second."


def apply_response_examples(schema: dict[str, Any]) -> None:
    set_json_example(schema, "get", "/health", "200", HEALTH_EXAMPLE)
    set_json_example(schema, "get", "/ready", "200", READY_OK)
    set_json_example(schema, "get", "/ready", "503", READY_DOWN)
    set_json_example(schema, "get", "/v1/alerts", "200", ALERT_LIST_EXAMPLE)
    set_json_example(schema, "get", "/v1/alerts/{alert_id}", "200", ALERT_DETAIL_EXAMPLE)
    set_json_example(schema, "patch", "/v1/alerts/{alert_id}", "200", ALERT_ACKNOWLEDGED)
    set_json_example(schema, "get", "/v1/rules", "200", [RULE_CARD, RULE_MERCHANT])


ERROR_CODES: dict[tuple[str, str], tuple[str, ...]] = {
    ("get", "/health"): ("500",),
    ("get", "/ready"): ("500",),
    ("get", "/v1/alerts"): ("422", "500"),
    ("get", "/v1/alerts/{alert_id}"): ("404", "422", "500"),
    ("patch", "/v1/alerts/{alert_id}"): ("403", "404", "409", "422", "500"),
    ("get", "/v1/rules"): ("500",),
    ("get", "/v1/stream/alerts"): ("422", "503", "500"),
}
