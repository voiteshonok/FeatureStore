# API changelog

Breaking and notable contract changes for the public `/v1/` surface (gateway).

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
  from `X-Operator-Id` / `X-Operator-Role` (set by the gateway after T7.3).
  `note` is required when resolving (3–500 characters).

Version path stays `/v1/` — no external clients yet.
