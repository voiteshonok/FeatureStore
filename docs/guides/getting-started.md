# Getting started

This guide takes you from a clean checkout to your first authenticated API call
in about ten minutes. Everything runs locally in Docker.

- Audience: a developer who integrates a scoring client or an operator tool.
- Result: a running stack, generated traffic, one feature lookup with an API key
  and one operator session with a cookie.
- Next: [Alert triage tutorial](alert-triage-tutorial.md).

## 1. Prerequisites

- Docker with Docker Compose v2
- `uv` and Python 3.12 (for `make` targets)
- `curl` and `jq`

## 2. Start the stack

```bash
make secret   # local HMAC and JWT secrets in infra/secrets/ (not committed)
make up       # brokers, databases, services, gateway, dashboard
make ps       # wait until every service is healthy
```

The public entry point is the **gateway** on `http://localhost:8080`. The
dashboard on `http://localhost:8081` proxies `/api/*` to the same gateway.
Other ports are internal and are closed in production.

Check that the gateway is ready:

```bash
curl -s http://localhost:8080/ready | jq
```

```json
{ "status": "ready", "features": "ok", "detection": "ok" }
```

`503` with `"status": "not_ready"` means a dependency is still starting; repeat
after a few seconds.

## 3. Generate traffic

Features and alerts only exist after payments flow through the system:

```bash
make demo    # simulator at 50 TPS with card bursts and bad merchants
```

Leave it running in a second terminal. After about a minute the first alerts
appear.

## 4. Choose how you authenticate

| Client | Credential | Where it goes | Can call |
| --- | --- | --- | --- |
| Scoring service (machine) | API key | `X-API-Key` header | `POST /v1/features` |
| Operator (human, dashboard or script) | Session cookie `fs_session` (JWT) | set by `POST /v1/auth/login` | alerts, rules, catalog, streams, `PATCH` by role |

Local values:

- API key: `dev-scoring-key` (client name `scoring`, from `GATEWAY_API_KEYS` in
  `infra/.env.example`).
- Operators: `viewer@featurestore.local` / `demo-viewer-pass`,
  `analyst@featurestore.local` / `demo-analyst-pass`,
  `admin@featurestore.local` / `demo-admin-pass`.

> **Security.** These credentials exist only in the local seed. In any shared
> environment set your own `GATEWAY_API_KEYS` and operator passwords, never
> commit them, and send credentials only over HTTPS.

## 5. First call: read features with an API key

Take a card token from any alert (see step 6) or from the simulator log, then:

```bash
curl -s http://localhost:8080/v1/features \
  -H 'X-API-Key: dev-scoring-key' \
  -H 'Content-Type: application/json' \
  -d '{
        "card_token": "<64-hex card token>",
        "merchant_id": "m7",
        "ts": "2026-11-24T18:00:00Z",
        "lat": 54.6872,
        "lon": 25.2797
      }' | jq
```

Response `200` (values shortened):

```json
{
  "request_id": "3b7f0d0e5c9a4c27a1b1f0f3d2a9e811",
  "features": {
    "card_cnt_1m": { "value": 4, "as_of": "2026-11-24T17:59:58Z", "age_ms": 2140, "status": "fresh" },
    "merchant_cnt_24h": { "value": 1312, "as_of": "2026-11-24T17:59:59Z", "age_ms": 1020, "status": "fresh" },
    "geo_velocity_kmh": { "value": 0.0, "as_of": null, "age_ms": null, "status": "fresh" }
  }
}
```

Every feature carries a `status`:

| status | Meaning | What to do |
| --- | --- | --- |
| `fresh` | Value is within its freshness budget | Use it |
| `stale` | Value is older than the budget | Use with care, log it |
| `fallback` | Online store unavailable, value from fallback path | Degrade the decision |
| `default` | No data for this entity | Treat as "unknown", not as zero risk |

Why `POST` for a read: the body contains a card token and coordinates. They must
not appear in URLs, access logs or proxy caches.

## 6. First operator call: log in and list alerts

```bash
# log in; the cookie is stored in cookies.txt
curl -s -c cookies.txt -o /dev/null -w '%{http_code}\n' \
  http://localhost:8080/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"analyst@featurestore.local","password":"demo-analyst-pass"}'
# → 204

curl -s -b cookies.txt http://localhost:8080/v1/auth/me | jq
```

```json
{ "operator_id": "0b6c…", "email": "analyst@featurestore.local", "role": "analyst" }
```

```bash
curl -s -b cookies.txt 'http://localhost:8080/v1/alerts?status=open&limit=2' | jq
```

The response is a keyset page: `items` and `next_cursor`. Copy a `card_token`
from an item to use in step 5.

## 7. When something fails

All errors use one body (Problem Details):

```json
{ "type": "unauthorized", "title": "missing credentials", "status": 401, "request_id": "…" }
```

Switch on `type`, show `title`, and quote `request_id` when you report a
problem. Full list: [error taxonomy](../api-errors.md). Client rules:
[best practices](best-practices.md).

## 8. Try the API without the stack

The committed gateway spec is enough to try requests before Docker is up:

```bash
make docs-mock   # Prism on http://localhost:4010, examples from gateway.json
```

`POST http://localhost:4010/v1/features` with either named example (`typical`
sends a card token and `m7`; `edge` sends only the merchant) returns the
example `200` body. Nothing is persisted. Stop the process with Ctrl-C, then
follow the sections above when you want a real vector.

## Where to go next

- [Alert triage tutorial](alert-triage-tutorial.md) — the full operator workflow
- [Best practices](best-practices.md) — retries, `type`, `request_id`, freshness
- [API reference](../openapi/gateway.json) — or Swagger UI at `http://localhost:8080/docs`
- [Live streams](../asyncapi/) — SSE contracts for alerts and system state
- [Changelog](../api-changelog.md)
