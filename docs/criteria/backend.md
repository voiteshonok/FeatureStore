# Criterion: Backend

## Architecture Decision Record

### Status

Status: Accepted · Date: 2026-11-21 · Category: **Backend (Python services)**

### Context

The monorepo runs four FastAPI services plus shared libraries. The thesis Backend
minimum asks for an ORM, layered architecture, global error handling, and a clean
dependency audit. Not every persistence path is a good fit for an ORM: the online
feature store is Redis with Lua, the offline store is ClickHouse batch inserts,
and detection's Kafka consumer collapses duplicate open alerts via partial unique
indexes.

### Decision

- **ORM (SQLAlchemy 2.0 async)** in the detection alert/rules CRUD API only
  (ADR-019). Models mirror tables created by `infra/postgres/` scripts;
  `create_all` is forbidden.
- **Layered packages** in detection: `api/` → `services/` → `repositories/` →
  `models/`. Dependencies are injected with FastAPI `Depends`.
- **No ORM** for: Redis window updates (Lua, ADR-002 / ADR-016 / ADR-017),
  ClickHouse offline writer batches (ADR-018), and the detection consumer insert
  path (raw SQL + partial unique indexes, ADR-003 / ADR-019).
- Shared message/API contracts stay in `libs/schemas` as Pydantic models.

### Alternatives considered

| Alternative | Pros | Cons | Why not chosen |
| --- | --- | --- | --- |
| ORM for Redis/ClickHouse | One style | Wrong abstraction; Lua and columnar batches are not row CRUD | Engines dictate access |
| Raw SQL in every FastAPI route | Fewer deps | Fails ORM + layering minimum; hard to test transitions | Detection CRUD moved to layers |
| ORM for consumer inserts | Uniform code | Obscures ON CONFLICT / partial-unique collapse | Hot path stays raw SQL |

### Consequences

Positive: dashboard CRUD is testable in layers; thesis ORM evidence is concrete.
Negative: two access styles inside detection (ORM API + raw consumer) — documented
in ADR-019 so the split is intentional, not accidental.

## Requirements checklist (rubric: Backend, minimum — partial)

| # | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Use an ORM | ✅ | SQLAlchemy 2.0 in `services/detection` CRUD (ADR-019) |
| 2 | Layered architecture | ✅ | `detection/api`, `services`, `repositories`, `models` |
| 3 | Global error handling | ✅ | Problem Details handlers in each service; [docs/api-errors.md](../api-errors.md) |
| 4 | Avoid vulnerable packages | ✅ | `make audit` / CI `uvx pip-audit` on `uv export --frozen` |
