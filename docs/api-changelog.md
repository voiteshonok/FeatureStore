# API changelog

Breaking and notable contract changes for the public `/v1/` surface (gateway).

## 2026-11-22 — Alert list for the operator UI (T8.4)

`AlertOut` now includes `acknowledged_by` and `resolved_by` (nullable). They were
already stored on `detection.alerts` and were missing from the response.

`GET /v1/alerts` accepts `entity_type=card|merchant`. `card` keeps rows with a
card token; `merchant` keeps rows with a merchant id. The existing `entity`
parameter is unchanged and still matches an id exactly.

## 2026-11-21 — System state stream (T7.5 / ADR-022)

Added `GET /v1/stream/system` (`text/event-stream`): SSE events `snapshot`,
`change`, and `: ping` every 15 s. Session operators only (same RBAC as alert
reads). Schemas in `libs/schemas.stream`; AsyncAPI at
`docs/asyncapi/system-stream.yaml`. OpenAPI does not describe the stream body.
Prometheus is part of the default compose stack for consumer lag; when it is
down, `lag` is `null` and HTTP-derived statuses still flow.

## 2026-11-21 — Live alert/stats stream (T7.4 / ADR-021)

Added `GET /v1/stream/alerts` (`text/event-stream`): SSE events `alert`, `stats`,
`reset`, and `: ping` every 15 s. Session operators only (ADR-020). Schemas in
`libs/schemas.stream`; short AsyncAPI at `docs/asyncapi/alerts-stream.yaml`.
OpenAPI does not describe the stream body.

## 2026-11-21 — Operator login (T7.3 / ADR-020)

**Breaking** for clients that used the `dashboard` API key.

- `POST /v1/auth/login` `{email, password}` → 204 + httpOnly cookie `fs_session`.
- `POST /v1/auth/logout` → 204, cookie cleared.
- `GET /v1/auth/me` → `{operator_id, email, role}` (401 without session).
- Dual auth: API key (scoring) or session cookie (operators). The `dashboard` key
  is removed from defaults. Cookie-authenticated mutating requests require
  `X-Requested-With: fetch`. Gateway sets `X-Operator-Id` / `X-Operator-Role` on
  upstream calls and strips any client-supplied copies.

## 2026-11-21 — Unified Problem Details errors (T7.2)

**Breaking** for clients that parsed `{"detail": ...}`.

Gateway-owned 401 / 403 / 429 / 503 / 504 and all service-local errors now use
`{type, title, status, request_id, errors}` (see [api-errors.md](api-errors.md)).
Upstream proxied bodies are unchanged. Timeouts at the gateway are **504**
(`upstream_timeout`); connect failures remain **503**.

## 2026-11-21 — Detection alerts list shape (T7.1)

**Breaking.** `GET /v1/alerts` no longer returns a bare JSON array.

- **Before:** `AlertOut[]`, single required `status` query param, `limit` only.
- **After:** `{ "items": AlertOut[], "next_cursor": string | null }` with filters
  `status` (repeatable), `entity`, `rule_id`, `created_from`, `created_to`,
  `sort` (`created_at` | `-created_at`), keyset `cursor`, and `limit`.

Also added:

- `GET /v1/alerts/{alert_id}` — alert, rule name/conditions, audit history.
- `GET /v1/rules` — rules with conditions.
- `PATCH /v1/alerts/{alert_id}` body is `{ "status", "note"? }`; operator comes
  from `X-Operator-Id` / `X-Operator-Role` (set by the gateway; ADR-020).
  `note` is required when resolving (3–500 characters).

Version path stays `/v1/` — no external clients yet.
