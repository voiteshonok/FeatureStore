# Criterion: Microservice Architecture

## Architecture Decision Record

### Status

Status: Accepted · Date: 2026-11-15

### Context

The pipeline has four stages with conflicting runtime profiles: event acceptance
must absorb bursts and broker outages without losing data (write-heavy, the
security boundary — the only place PANs exist); feature serving must answer in
milliseconds and scale with read traffic; detection evolves fastest (rules today,
ML scoring later) and must not endanger the serving path; the edge needs authn
and rate limiting that do not belong in business code. The thesis subject —
fault tolerance — requires these stages to fail *independently* and recover from
the event log, which a modular monolith cannot demonstrate.

### Decision

Three business services — **ingestion**, **features**, **detection** — plus an
edge **gateway**, in one monorepo (uv workspace), each with its own package,
Docker image, database schema and credentials. The features service ships as two
images from one package — `worker` (consumers) and `api` (serving) — so consumer
load never touches API latency (ADR-007). Asynchronous backbone over Kafka
(`transactions.raw`, `features.updates`) with versioned frozen contracts in
`libs/schemas`; synchronous REST only at the edge (gateway → features-api /
detection), max chain length 2. Full picture:
[docs/architecture.md](../architecture.md).

### Alternatives considered

| Alternative | Pros | Cons | Why not chosen |
| --- | --- | --- | --- |
| Modular monolith | One deploy, simpler ops | Shared failure domain; cannot show independent recovery/replay; one scaling axis | Contradicts the thesis subject and the read/write asymmetry |
| 2 services (ingest + everything else) | Fewer moving parts | Detection failures would sit inside the serving process; rule redeploys restart serving | Serving-path isolation is the point |
| Fine-grained services (per feature family / rule set) | "More micro" | Entity-service antipattern; chatty sync calls; operational overkill | Decomposition follows bounded contexts, not tables |
| Off-the-shelf gateway (Traefik/Kong) | Mature routing/TLS | Harder to defend line-by-line; forward-auth still needs a service | Thin custom FastAPI gateway (ADR-008): small, testable, fully explainable |

### Consequences

Positive: independent failure domains and replayable recovery (demonstrable
experiments); per-service scaling; clear data ownership. Negative: more
deployment units and contracts to maintain — mitigated by the monorepo, shared
CI and contract tests. Neutral: `libs/schemas` must stay a contracts-only
package — versioned frozen message/API models and the declarative catalog, the
equivalent of an OpenAPI/AsyncAPI artifact; all behaviour lives in the owning
service.

## Requirements checklist (rubric: minimum)

| # | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Architecture diagram: services, interactions, protocols | ✅ | docs/architecture.md |
| 2 | Domain model: bounded contexts | ✅ | docs/architecture.md §Bounded contexts |
| 3 | API contracts between services (OpenAPI/AsyncAPI) | ✅ | docs/openapi/{ingestion,features,detection,gateway}.json; Kafka contracts: versioned frozen models in libs/schemas (`TransactionEvent`, `FeatureSnapshot`) + contract tests |
| 4 | Deployment diagram | ✅ | docs/architecture.md §Deployment |
| 5 | Decomposition rationale | ✅ | This ADR + docs/architecture.md |
| 6 | ≥3 business services, each its own domain | ✅ | services/ingestion, services/features (worker+api), services/detection; gateway as edge |
| 7 | Loose coupling; services work independently | ✅ | Kafka backbone; no sync calls between business services; windows rebuild from replay |
| 8 | No shared database (or justified) | ✅ | Schema-per-service + per-service credentials (infra/postgres, docs/db/data-dictionary.md) |
| 9 | Sync: REST with versioned contracts | ✅ | `/v1/` on every API; gateway proxies only |
| 10 | Timeouts and retry policies on all sync calls | ✅ | services/gateway proxy: connect/read timeouts, bounded retries for idempotent GET only |
| 11 | Handling of unavailable dependencies | ✅ | ingestion: 503+Retry-After on Kafka outage (ADR-006, tested); gateway: upstream down → 503, no hang; worker pauses without committing offsets on Redis outage |
| 12 | Rate limiting at the entry point | ✅ | gateway: slowapi per API key (`GATEWAY_RATE_LIMIT`), 429 + Retry-After |
| 13 | Own codebase per service (monorepo ok) | ✅ | services/* as uv workspace packages |
| 14 | Separate Docker image per service | ✅ | 5 images: ingestion, features worker + api (Dockerfile, Dockerfile.api — ADR-007), detection, gateway |
| 15 | Own DB / isolated schema per service | ✅ | infra/postgres: schemas + svc_* accounts, least-privilege grants |
| 16 | Health/ready endpoints | ✅ | `/health` + `/ready` with per-dependency probes in every service; compose healthchecks gate startup |
| 17 | Correlation-id logging | ✅ | `event_id` carried end-to-end (event → snapshot → alert); `X-Request-ID` at the gateway; single-line JSON logs, PAN-masked |
| 18 | API gateway: single entry, routing, authn, rate limit | ✅ | services/gateway (ADR-008) |
| 19 | Graceful degradation, partial results, correct codes, centralized error logging | ✅ | default/stale feature statuses (ADR-013); 503/429/409 paths; JSON error logs |
| 20 | Unit tests per service; integration for critical flows; contract tests | ✅ | unit tests in every service; contract tests in libs/schemas; e2e: tests/e2e/test_stage2.py, test_stage3_features.py |

**Restrictions compliance:** no sync chains >3 (max is 2: client → gateway →
upstream) · no access to another service's DB (schemas + grants make it
impossible) · no shared business-logic library (`libs/schemas` = contracts only,
argued above) · no distributed transactions (at-least-once + idempotent
consumers and schema-level dedup instead, ADR-003) · not a folder-split monolith
(independent images, independent failure and recovery, demonstrated by replay).

## Maximum-level directions

| Direction | Implemented |
| --- | --- |
| Asynchronous communication | Event-driven backbone (`transactions.raw`, `features.updates`); idempotent producer (ingestion); duplicate-safe apply via per-partition watermarks in one atomic Lua script (ADR-003); recovery by replay from committed offsets |
| Resilience patterns | Timeout management + bounded retries (gateway); graceful degradation with explicit feature statuses (ADR-013); 503+Retry-After backpressure (ADR-006); freshness heartbeat separating "no traffic" from "writer down" |
| Observability (partial) | Correlation ids end-to-end, structured JSON logs, health/ready with per-dependency detail; Prometheus metrics on METRICS_PORT (ADR-014) |
