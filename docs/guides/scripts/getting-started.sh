#!/usr/bin/env bash
# Curl half of docs/guides/getting-started.md. The stack must already be up.
set -euo pipefail

API="${API:-http://localhost:8080}"
KEY="${API_KEY:-dev-scoring-key}"

code="000"
for _ in $(seq 1 30); do
  code="$(curl -s -o /tmp/fs-ready.json -w '%{http_code}' "$API/ready" || true)"
  if [[ "$code" == "200" ]]; then
    break
  fi
  sleep 2
done
test "$code" = "200"

uv run python - "$API" "$KEY" <<'PY'
import json
import sys
import urllib.request

api, key = sys.argv[1], sys.argv[2]
body = {
    "card_token": "c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00c0ffee00",
    "merchant_id": "m7",
    "ts": "2026-11-24T18:00:00.000Z",
    "lat": 54.6872,
    "lon": 25.2797,
}
features = urllib.request.Request(
    api + "/v1/features",
    data=json.dumps(body).encode(),
    headers={"Content-Type": "application/json", "X-API-Key": key},
    method="POST",
)
with urllib.request.urlopen(features, timeout=15) as response:
    assert response.status == 200
    payload = json.load(response)
    assert "features" in payload

login = urllib.request.Request(
    api + "/v1/auth/login",
    data=json.dumps(
        {"email": "analyst@featurestore.local", "password": "demo-analyst-pass"}
    ).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(login, timeout=15) as response:
    assert response.status == 204
    cookie = response.headers.get("Set-Cookie", "")
    token = cookie.split("fs_session=", 1)[1].split(";", 1)[0]

me = urllib.request.Request(
    api + "/v1/auth/me",
    headers={"Cookie": f"fs_session={token}"},
)
with urllib.request.urlopen(me, timeout=15) as response:
    assert response.status == 200
    operator = json.load(response)
    assert operator["email"] == "analyst@featurestore.local"
    assert operator["role"] == "analyst"
print("getting-started ok")
PY
