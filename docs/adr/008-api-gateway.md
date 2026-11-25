# ADR-008: API gateway

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** NFR-9, US-6.5; complexity criterion "Microservices"; ADR-012

## Context

External clients (scoring service, dashboard) need one entry point with authentication
(JWT for dashboard users, API keys for the scoring service), routing to internal
services, rate limiting (100 requests per second per client) and TLS. The gateway adds
a network hop to the feature read path, so its latency counts towards K1.

The ingestion write path is a separate published port. Event volume and backpressure
stay on that service.

## Alternatives considered

- **Custom FastAPI gateway** — full control, easy to explain in the thesis, more code
  to write and test.
- **Traefik with forward-auth** — mature routing, TLS and rate limiting out of the box;
  authentication stays in a small auth service.

## Decision

Accepted on 2026-11-15: a thin custom FastAPI gateway.

The gateway is the entry point for read and control traffic. It checks API keys, limits
each client to 100 requests per second, and proxies to the feature API and the detection
API. It does not compute features. TLS termination stays outside the process
(compose-internal HTTP). Operator JWT-in-cookie auth is defined in ADR-020 (superseding
the earlier “JWT outside diploma scope” line); OIDC remains out of scope. Machine
callers present API keys.

Ingestion keeps published port 8000. The gateway does not route `POST /v1/events`, so
write-path backpressure and delivery stay at ingestion.

The feature catalog is read from Postgres. features-api exposes `POST /v1/features` and
health only (ADR-007, ADR-012), so the gateway runs one `SELECT` as `svc_gateway`.

## Consequences

- Clients call port 8080 for features, alerts, and the catalog. Ingestion stays on
  port 8000. features-api and detection keep their compose ports until every client
  uses the gateway.
- `GATEWAY_API_KEYS` holds comma-separated `client:key` pairs. A missing key is 401
  and an unknown key is 403. The client name is the rate-limit identity and is written
  on the request log.
- The 100 requests/second budget is one bucket per client across the proxied routes,
  enforced in process by slowapi. One gateway replica is the deployed shape; another
  replica would have its own bucket.
- Upstream calls use a 0.5 s connect timeout and a 2 s read timeout. GET is tried
  three times, with 0.1 s then 0.4 s between attempts. `POST /v1/features` is an
  idempotent read: one extra attempt after 0.05 s, and only on `ConnectError`
  (the request never left the gateway). Other POST and PATCH calls are not
  retried. A timeout becomes 504; any other transport failure becomes 503. An
  upstream HTTP response, including 4xx, is returned unchanged. **Exception
  (ADR-021):** SSE routes use a streaming proxy with no read timeout and no
  retries.
- `svc_gateway` has `SELECT` on the feature registry tables and nothing else. A failed
  catalog read is 503.
- The extra hop is on the feature read path and counts toward K1.
