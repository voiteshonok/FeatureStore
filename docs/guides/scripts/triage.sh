#!/usr/bin/env bash
# Repeat docs/guides/alert-triage-tutorial.md against a running stack.
# Payments go to ingestion. The gateway never sees a PAN.
set -euo pipefail

API="${API:-http://localhost:8080}"
INGESTION="${INGESTION:-http://localhost:8000}"

uv run python - "$API" "$INGESTION" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

api, ingestion = sys.argv[1], sys.argv[2]
NOTE = "Confirmed with cardholder, legitimate travel"


def request(method: str, url: str, body: dict | None = None, headers: dict | None = None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            raw = response.read()
            payload = json.loads(raw) if raw else None
            return response.status, payload, response.headers
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        payload = json.loads(raw) if raw else None
        return exc.code, payload, exc.headers


def login() -> str:
    status, _payload, headers = request(
        "POST",
        api + "/v1/auth/login",
        {"email": "analyst@featurestore.local", "password": "demo-analyst-pass"},
        {"Content-Type": "application/json"},
    )
    if status != 204:
        raise SystemExit(f"login failed: {status}")
    cookie = headers.get("Set-Cookie", "")
    return cookie.split("fs_session=", 1)[1].split(";", 1)[0]


def session_headers(token: str, *, csrf: bool = False) -> dict[str, str]:
    headers = {"Cookie": f"fs_session={token}"}
    if csrf:
        headers["Content-Type"] = "application/json"
        headers["X-Requested-With"] = "fetch"
    return headers


def open_alerts(token: str) -> list[dict]:
    status, payload, _headers = request(
        "GET",
        api + "/v1/alerts?status=open&limit=20",
        headers=session_headers(token),
    )
    if status != 200 or not isinstance(payload, dict):
        raise SystemExit(f"list alerts failed: {status} {payload}")
    items = payload.get("items")
    if not isinstance(items, list):
        raise SystemExit("alert page has no items array")
    return items


def burst() -> None:
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    for _ in range(10):
        payment = {
            "event_id": str(uuid.uuid4()),
            "payment_id": "triage-" + uuid.uuid4().hex[:8],
            "pan": "4111111111111111",
            "merchant_id": "m7",
            "amount_minor": 2599,
            "currency": "EUR",
            "status": "approved",
            "event_time": now,
            "lat": 54.6872,
            "lon": 25.2797,
        }
        status, payload, _headers = request(
            "POST",
            ingestion + "/v1/events",
            payment,
            {"Content-Type": "application/json"},
        )
        if status != 202:
            raise SystemExit(f"ingestion rejected the burst: {status} {payload}")


token = login()
items = open_alerts(token)
if not items:
    burst()
    deadline = time.time() + 90
    while time.time() < deadline:
        time.sleep(2)
        items = open_alerts(token)
        if items:
            break
if not items:
    raise SystemExit("no open alert after the ingestion burst")

alert_id = items[0]["alert_id"]
status, detail, _headers = request(
    "GET",
    f"{api}/v1/alerts/{alert_id}",
    headers=session_headers(token),
)
if status != 200:
    raise SystemExit(f"alert detail failed: {status} {detail}")

status, acked, _headers = request(
    "PATCH",
    f"{api}/v1/alerts/{alert_id}",
    {"status": "acknowledged"},
    session_headers(token, csrf=True),
)
if status != 200 or not isinstance(acked, dict) or acked.get("status") != "acknowledged":
    raise SystemExit(f"acknowledge failed: {status} {acked}")

status, resolved, _headers = request(
    "PATCH",
    f"{api}/v1/alerts/{alert_id}",
    {"status": "resolved", "note": NOTE},
    session_headers(token, csrf=True),
)
if status != 200 or not isinstance(resolved, dict) or resolved.get("status") != "resolved":
    raise SystemExit(f"resolve failed: {status} {resolved}")

status, _payload, _headers = request(
    "POST",
    api + "/v1/auth/logout",
    headers=session_headers(token, csrf=True),
)
if status != 204:
    raise SystemExit(f"logout failed: {status}")
print("triage ok")
PY
