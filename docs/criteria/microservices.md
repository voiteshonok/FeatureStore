# Criterion: Microservice Architecture

## Architecture Decision Record

### Status

Status: Accepted · Date: 2026-11-15

### Context

The pipeline has four stages with conflicting runtime profiles: event acceptance
must absorb bursts and broker outages without losing data (write-heavy, security
boundary — the only place PANs exist); feature serving must answer in milliseconds
and scale with read traffic; detection evolves fastest (rules today, ML scoring
later) and must not endanger the serving path; the edge needs authn and rate
limiting that do not belong in business code. The thesis subject — fault
tolerance — requires these stages to fail *independently* and recover from the
event log, which a modular monolith cannot demonstrate.

### Decision

Four services — **ingestion**, **features**, **detection**, **gateway** — in one
monorepo (uv workspace), each with its own package, Dockerfile, database schema and
credentials. Asynchronous backbone over Kafka (`transactions.raw`,
`features.updates`) with versioned frozen contracts in `libs/schemas`; synchronous
REST only at the edge (gateway → features/detection), max chain length 2. Full
picture: [docs/architecture.md](../architecture.md).

### Alternatives considered

| Alternative | Pros | Cons | Why not chosen |
| --- | --- | --- | --- |
| Modular monolith | One deploy, simpler ops, no network between modules | Shared failure domain; cannot show independent recovery/replay; one scaling axis | Contradicts the thesis subject (fault tolerance) and the read/write asymmetry |
| 2 services (ingest + everything else) | Fewer moving parts | Detection failures would sit inside the serving process; rules/ML redeploys restart serving | Serving-path isolation is the point |
| Fine-grained services (per feature family, per rule set) | "More micro" | Entity-service antipattern; chatty sync calls; operational overkill for the domain | Decomposition should follow bounded contexts, not tables |
| Off-the-shelf gateway (Traefik/Kong) vs custom FastAPI gateway | Mature routing/TLS | Harder to defend line-by-line in a diploma; forward-auth still needs a service | Custom thin gateway chosen (ADR-008): small, testable, fully explainable |

### Consequences

Positive: independent failure domains and replayable recovery (demonstrable
experiments); per-service scaling; clear ownership of data. Negative: more
deployment units and contracts to maintain — mitigated by the monorepo, shared CI,
and contract tests. Neutral: `libs/schemas` must stay a contracts-only package
(enforced by review; no business logic imports).

## Implementation map

| Rubric area | Where |
| --- | --- |
| Architecture & deployment diagrams, domain model, decomposition rationale | docs/architecture.md |
| Contracts: OpenAPI per service, Kafka message schemas | docs/openapi/ (exported), libs/schemas + contract tests |
| Health/ready, JSON logs, correlation id | each service (`/health`, `/ready`; `event_id` / `X-Request-ID`) |
| Timeouts, retries, degradation on sync calls | gateway HTTP client policy; ADR-006 pattern |
| Rate limiting & authn at entry point | gateway (API key + per-client limit) |
| Per-service DB isolation | infra/postgres: schemas + svc_* accounts (docs/db/data-dictionary.md) |
| Error handling | 503+Retry-After (ingestion, ADR-006); degraded responses at gateway; correct codes everywhere |
| Tests | unit per service; integration (testcontainers) for ingestion flow; contract tests in libs/schemas |

## Requirements checklist (rubric: minimum)

| # | Requirement | Status | Evidence / plan |
| --- | --- | --- | --- |
| 1 | Architecture diagram: services, interactions, protocols | ✅ | docs/architecture.md |
| 2 | Domain model: bounded contexts | ✅ | docs/architecture.md §Bounded contexts |
| 3 | API contracts between services (OpenAPI/AsyncAPI) | ⚠️ | ingestion OpenAPI exported ✅; Kafka contracts as versioned Pydantic models + topic table ✅; features/gateway OpenAPI exported when services land (ETA below) |
| 4 | Deployment diagram | ✅ | docs/architecture.md §Deployment |
| 5 | Decomposition rationale | ✅ | This ADR + architecture.md |
| 6 | ≥3 business services, each its own domain | ⚠️ | ingestion ✅ done; features, detection — in progress (stage 3), ETA before the review slot |
| 7 | Loose coupling; services work independently | ✅ (by design) | Kafka backbone; no sync calls between business services |
| 8 | No shared database (or justified) | ✅ | Schema-per-service + per-service credentials |
| 9 | Sync: REST, versioned contracts | ✅ (design) / ⚠️ (code) | `/v1/` everywhere; gateway pending |
| 10 | Timeouts and retry policies on all sync calls | ⚠️ | Gateway client policy specified; lands with gateway |
| 11 | Handling of unavailable dependencies | ✅ / ⚠️ | ingestion: 503+Retry-After on Kafka outage ✅ (ADR-006, tested); gateway degradation pending |
| 12 | Rate limiting at entry point | ⚠️ | Gateway (slowapi, per-client) — pending |
| 13 | Own codebase per service (monorepo ok) | ✅ | services/* as uv workspace packages |
| 14 | Separate Docker image per service | ⚠️ | ingestion, simulator ✅; features/detection/gateway Dockerfiles follow the same template |
| 15 | Own DB / isolated schema per service | ✅ | infra/postgres |
| 16 | Health/ready endpoints | ⚠️ | ingestion ✅; same pattern for the rest |
| 17 | Correlation-id logging | ⚠️ | ingestion JSON logs ✅; `event_id` propagation + `X-Request-ID` middleware for the rest |
| 18 | API gateway: single entry, routing, authn, rate limit | ⚠️ | ADR-008 accepted (custom FastAPI); implementation pending |
| 19 | Graceful degradation, partial results, correct codes, centralized error logging | ⚠️ | Pattern established in ingestion; to be mirrored |
| 20 | Unit tests per service; integration for critical flows; contract tests | ⚠️ | ingestion unit+integration ✅; schemas contract tests ✅; features/detection/gateway tests land with the code |

**Restrictions compliance:** no sync chains >3 (max is 2) · no access to another
service's DB (schemas + grants make it impossible) · no shared business-logic
library (`libs/schemas` = contracts only, argued above) · no distributed
transactions (at-least-once + idempotent consumers instead, ADR-003) · not a
folder-split monolith (independent images, independent failure/recovery).

## Plan to close ⚠️ items (before the expert slot)

1. **features** service: Kafka consumer → Redis windows (ADR-002) → `POST /v1/features` (contract already in `libs/schemas`), registry upsert, health/ready, Dockerfile, tests.
2. **detection** service: consume `features.updates`, evaluate rules from Postgres, write alerts, health/ready, Dockerfile, tests.
3. **gateway**: routing, API key, rate limit, timeouts/retries, `X-Request-ID`, OpenAPI export, Dockerfile, tests.
4. Compose entries + per-service DB credentials; integration test of the full flow (event → feature → alert).

Work items are specified in `specs/stage3-services.md`; the checklist above is
updated as items land.
