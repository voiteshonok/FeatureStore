# ADR-019: ORM in the detection API

- **Status:** Accepted
- **Date:** 2026-11-21
- **Related:** ADR-003, ADR-007, ADR-008

## Context

The detection alert API mixed six raw SQL strings with FastAPI routes. The thesis
minimum for Backend requires an ORM and a layered layout. The same service also
runs the Kafka consumer that inserts alerts under at-least-once redelivery
(ADR-003); that path depends on partial unique indexes and must stay race-safe.

## Decision

- **SQLAlchemy 2.0**, async sessions, driver `postgresql+psycopg` (psycopg 3 —
  already used by the consumer pool; no second driver).
- ORM models map existing tables in `detection.*` and `ops.audit_log`. Schema is
  created only by scripts under `infra/postgres/`. Application code never calls
  `create_all`.
- **ORM only for the CRUD API** (list/get/patch alerts, list rules). Layers:
  `api/` → `services/` → `repositories/` → `models/`. Routers do not see
  sessions; services do not embed SQL.
- **Consumer stays on raw SQL** (`rules._insert_alert`): the hot path needs a
  single insert that collapses on the partial unique indexes for one open alert
  per subject. An ORM upsert would not improve clarity and could hide the
  conflict semantics the schema already owns.
- Operator identity for status changes comes from gateway headers
  `X-Operator-Id` / `X-Operator-Role` (JWT cookie; ADR-020), not from the
  request body.

## Alternatives considered

- **ORM everywhere, including the consumer** — fights the partial-unique collapse
  pattern and adds session overhead on every snapshot.
- **Keep raw SQL in routes** — fails the Backend ORM / layering minimum.
- **Separate detection-api and detection-worker images** — rejected earlier for
  alert volume (ADR-007 note in `api.py`); unchanged.

## Consequences

- Dashboard list/detail/rules go through typed repositories and a transition
  service (role checks, resolution notes, keyset cursors).
- Redis window Lua and ClickHouse offline batches remain without ORM — different
  engines and access patterns; see `docs/criteria/backend.md`.
- New DDL for the alerts UI ships as `infra/postgres/40-alerts-ui.sql`, not as a
  rewrite of `10-schema.sql`.
