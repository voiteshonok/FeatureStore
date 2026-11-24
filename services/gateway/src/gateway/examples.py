"""Named request and response examples for the gateway OpenAPI document.

Values match the guides and the local seed (``infra/postgres/20-seed.sql``).
AD-4 imports these dicts and replays them against a running stack.
"""

from __future__ import annotations

from typing import Any

from schemas.openapi_problems import set_json_example

SAMPLE_CARD = "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00"
ALERT_ID = "6a4e2f0c-1d8b-4a37-9b9e-2c7f5e1a0b44"
EVENT_ID = "3b7f0d0e-5c9a-4c27-a1b1-f0f3d2a9e811"
REQUEST_ID = "3b7f0d0e-5c9a-4c27-a1b1-f0f3d2a9e812"
RULE_CARD_ID = "c0ffee00-0000-4000-8000-000000000001"
RULE_MERCHANT_ID = "c0ffee00-0000-4000-8000-000000000003"
TS = "2026-11-24T18:00:00.000Z"
OBSERVED_AT = "2026-11-24T18:01:12.418Z"
RESOLVE_NOTE = "Confirmed with cardholder, legitimate travel"

LOGIN_TYPICAL: dict[str, Any] = {
    "email": "analyst@featurestore.local",
    "password": "demo-analyst-pass",
}
LOGIN_EDGE: dict[str, Any] = {"email": "a@b", "password": "x"}

FEATURE_TYPICAL: dict[str, Any] = {
    "card_token": SAMPLE_CARD,
    "merchant_id": "m7",
    "ts": TS,
    "lat": 54.6872,
    "lon": 25.2797,
}
FEATURE_EDGE: dict[str, Any] = {"merchant_id": "m7", "ts": TS}

PATCH_ACKNOWLEDGE: dict[str, Any] = {"status": "acknowledged"}
PATCH_RESOLVE: dict[str, Any] = {"status": "resolved", "note": RESOLVE_NOTE}

LOGIN_EXAMPLES: dict[str, dict[str, Any]] = {
    "typical": {"summary": "Seeded analyst", "value": LOGIN_TYPICAL},
    "edge": {"summary": "Shortest schema-valid credentials", "value": LOGIN_EDGE},
}
FEATURE_EXAMPLES: dict[str, dict[str, Any]] = {
    "typical": {"summary": "Card and merchant", "value": FEATURE_TYPICAL},
    "edge": {"summary": "Merchant only", "value": FEATURE_EDGE},
}
PATCH_EXAMPLES: dict[str, dict[str, Any]] = {
    "typical": {"summary": "Acknowledge an open alert", "value": PATCH_ACKNOWLEDGE},
    "edge": {"summary": "Resolve with a note", "value": PATCH_RESOLVE},
}

LOGIN_COOKIE = (
    "fs_session=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyIn0.sig; "
    "HttpOnly; Max-Age=3600; Path=/api; SameSite=strict"
)
LOGOUT_COOKIE = "fs_session=; HttpOnly; Max-Age=0; Path=/api; SameSite=strict"

ME_EXAMPLE: dict[str, Any] = {
    "operator_id": "2",
    "email": "analyst@featurestore.local",
    "role": "analyst",
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

CATALOG_EXAMPLE: list[dict[str, Any]] = [
    {
        "name": "card_cnt_1m",
        "entity": "card",
        "kind": "stream",
        "agg": "count",
        "default_value": 0,
        "version": 1,
        "window_ms": 60000,
        "stale_after_ms": 5000,
    },
    {
        "name": "geo_velocity_kmh",
        "entity": "card",
        "kind": "on_demand",
        "agg": "geo_velocity",
        "default_value": None,
        "version": 1,
        "window_ms": None,
        "stale_after_ms": None,
    },
]

HEALTH_EXAMPLE: dict[str, str] = {"status": "ok"}
READY_OK: dict[str, str] = {"status": "ready", "features": "ok", "detection": "ok"}
READY_DOWN: dict[str, str] = {"status": "not_ready", "features": "down", "detection": "ok"}

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

SYSTEM_SSE_EXAMPLE = (
    "event: snapshot\n"
    "data: "
    '{"mode":"normal","components":['
    '{"name":"gateway","status":"up","checked_at":"2026-11-24T18:01:12.418Z","lag":null}'
    "]}\n"
    "\n"
)

API_DESCRIPTION = """Public HTTP API of the feature store (`/v1`).

Machine clients send `X-API-Key` and call `POST /v1/features`. Operators send the
`fs_session` cookie and call alerts, rules, the catalog, and the live streams.

- Getting started: docs/guides/getting-started.md
- Alert triage: docs/guides/alert-triage-tutorial.md
- Errors: docs/api-errors.md
- Changelog: docs/api-changelog.md
- Architecture: docs/architecture.md
"""

OPENAPI_TAGS: list[dict[str, str]] = [
    {"name": "auth", "description": "Operator login, logout, and the current session."},
    {"name": "features", "description": "Feature lookup for scoring clients and operators."},
    {"name": "alerts", "description": "Alert list, detail, and status transitions."},
    {"name": "rules", "description": "Alert rules stored as data."},
    {"name": "catalog", "description": "Feature registry rows."},
    {
        "name": "stream",
        "description": "Server-Sent Events. Event payloads are specified in the AsyncAPI files.",
    },
    {"name": "ops", "description": "Process health and readiness probes."},
]

LOGIN_DESCRIPTION = (
    "Exchange an operator email and password for a session cookie. "
    "Login does not require a CSRF header. "
    "The limit is 5 attempts per minute per IP. "
    "A failed login returns the same 401 whether the email is unknown or the password is wrong."
)
LOGOUT_DESCRIPTION = (
    "Clear the fs_session cookie. "
    "Requires the session cookie and the CSRF header X-Requested-With: fetch."
)
ME_DESCRIPTION = "Return the operator id, email, and role for the current session."
FEATURES_DESCRIPTION = (
    "Read the feature vector for a card, a merchant, or both. "
    "POST is used because the body carries a card token and coordinates: "
    "they must not appear in URLs, access logs, or proxy caches. "
    "An API key or a session cookie is accepted."
)
LIST_ALERTS_DESCRIPTION = (
    "List alerts with keyset pagination. Requires a session cookie. An API key is not accepted."
)
GET_ALERT_DESCRIPTION = (
    "Fetch one alert with its rule and audit history. Requires a session cookie."
)
PATCH_DESCRIPTION = (
    "Change an alert status. Requires a session cookie, role analyst or admin, "
    "and the CSRF header X-Requested-With: fetch. "
    "Allowed transitions: open to acknowledged, acknowledged to resolved, "
    "and open to resolved. resolved is final. "
    "A note of 3–500 characters is required when resolving."
)
RULES_DESCRIPTION = (
    "List alert rules and their conditions. Rules are data. Requires a session cookie."
)
CATALOG_DESCRIPTION = "List feature definitions and versions. Requires a session cookie."
STREAM_ALERTS_DESCRIPTION = (
    "Server-Sent Events of alert and stats frames. Requires a session cookie. "
    "Payloads: docs/asyncapi/alerts-stream.yaml."
)
STREAM_SYSTEM_DESCRIPTION = (
    "Server-Sent Events of system state (snapshot and change). Requires a session cookie. "
    "Payloads: docs/asyncapi/system-stream.yaml."
)
HEALTH_DESCRIPTION = "The process is up. This probe does not check dependencies."
READY_DESCRIPTION = (
    "Features and detection each answer /health within one second. "
    "503 uses the same body with status not_ready."
)
LOGIN_204_DESCRIPTION = (
    "Session cookie set. The response has no body. "
    "Set-Cookie: fs_session, HttpOnly, SameSite=Strict, Max-Age=3600, Path=/api. "
    "Secure is added when cookie_secure is on."
)
LOGOUT_204_DESCRIPTION = (
    "Session cookie cleared (Max-Age=0, Path=/api, HttpOnly, SameSite=Strict). "
    "The response has no body."
)

SEC_PUBLIC: dict[str, list[dict[str, list[str]]]] = {"security": []}
SEC_SESSION: dict[str, list[dict[str, list[str]]]] = {"security": [{"SessionCookie": []}]}
SEC_EITHER: dict[str, list[dict[str, list[str]]]] = {
    "security": [{"ApiKey": []}, {"SessionCookie": []}],
}


def apply_response_examples(schema: dict[str, Any]) -> None:
    """Restore response examples after OpenAPI encoding strips nulls."""
    set_json_example(schema, "get", "/health", "200", HEALTH_EXAMPLE)
    set_json_example(schema, "get", "/ready", "200", READY_OK)
    set_json_example(schema, "get", "/ready", "503", READY_DOWN)
    set_json_example(schema, "get", "/v1/auth/me", "200", ME_EXAMPLE)
    set_json_example(schema, "post", "/v1/features", "200", FEATURE_RESPONSE_EXAMPLE)
    set_json_example(schema, "get", "/v1/alerts", "200", ALERT_LIST_EXAMPLE)
    set_json_example(schema, "get", "/v1/alerts/{alert_id}", "200", ALERT_DETAIL_EXAMPLE)
    set_json_example(schema, "patch", "/v1/alerts/{alert_id}", "200", ALERT_ACKNOWLEDGED)
    set_json_example(schema, "get", "/v1/rules", "200", [RULE_CARD, RULE_MERCHANT])
    set_json_example(schema, "get", "/v1/catalog", "200", CATALOG_EXAMPLE)


ERROR_CODES: dict[tuple[str, str], tuple[str, ...]] = {
    ("get", "/health"): ("500",),
    ("get", "/ready"): ("500",),
    ("post", "/v1/auth/login"): ("401", "422", "429", "503", "500"),
    ("post", "/v1/auth/logout"): ("401", "403", "429", "500"),
    ("get", "/v1/auth/me"): ("401", "503", "500"),
    ("post", "/v1/features"): ("401", "403", "422", "429", "503", "504", "500"),
    ("get", "/v1/alerts"): ("401", "403", "422", "429", "503", "504", "500"),
    ("get", "/v1/alerts/{alert_id}"): ("401", "403", "404", "422", "429", "503", "504", "500"),
    ("patch", "/v1/alerts/{alert_id}"): (
        "401",
        "403",
        "404",
        "409",
        "422",
        "429",
        "503",
        "504",
        "500",
    ),
    ("get", "/v1/rules"): ("401", "403", "429", "503", "504", "500"),
    ("get", "/v1/catalog"): ("401", "403", "429", "503", "500"),
    ("get", "/v1/stream/alerts"): ("401", "403", "429", "503", "500"),
    ("get", "/v1/stream/system"): ("401", "403", "429", "503", "500"),
}
