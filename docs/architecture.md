# Architecture

System: fault-tolerant real-time feature storage and serving for anomaly detection in
payment transaction streams. This page is the entry point for the Microservices and
Database evaluation criteria; decisions are recorded in [ADRs](adr/README.md).

## Bounded contexts and decomposition rationale

| Service | Bounded context | Owns (data) | Why a separate service |
| --- | --- | --- | --- |
| **ingestion** | Event acceptance: validation, PAN tokenization, deduplication, publish | `ingestion` schema (idempotency keys) | Security boundary — the only component that ever sees a PAN; must absorb client bursts and broker outages (ADR-006) independently of the read path |
| **features** | Feature computation and serving: sliding windows, online store, offline copy | Redis keys, ClickHouse tables, `features` schema (registry) | Latency-critical read path (P99 ms-level); scales with read traffic, not with ingest; independent online/offline writers (ADR-001) |
| **detection** | Anomaly detection: rule evaluation, alert lifecycle | `detection` schema (rules, alerts) | Evolves independently (rules → ML model later); failure must not affect feature serving |
| **gateway** | Edge: routing, authn, rate limiting | — (stateless) | Single entry point; keeps cross-cutting edge concerns out of business services (ADR-008) |
| simulator | Load generation (dev/test only) | — | Reproducible traffic for demos and experiments |

Decomposition follows the data-flow stages of the domain, not entities: each service
has a distinct failure mode, load profile and scaling axis — the core claim of the
thesis (fault tolerance) is only demonstrable if the stages fail independently.

**No shared database:** each service owns its schema with per-service credentials and
least-privilege grants (see [db/data-dictionary.md](db/data-dictionary.md)). Redis and
ClickHouse are owned by the features service exclusively.

**`libs/schemas` is a contract package, not a shared business-logic library:** it
contains only versioned, frozen message/API models (Pydantic) and the declarative
feature catalog — the equivalent of an OpenAPI/AsyncAPI artifact, consumed by all
sides of each contract and covered by contract tests. All behaviour (tokenization,
windowing, rule evaluation) lives inside the owning service.

## Component diagram

```mermaid
flowchart TB
    subgraph edge [Edge]
        GW["gateway<br/>authn, routing, rate limit"]
    end
    subgraph core [Business services]
        ING["ingestion<br/>:8000"]
        FEAT["features<br/>:8001"]
        DET["detection<br/>:8002"]
    end
    subgraph infra [Infrastructure]
        K[(Kafka)]
        R[(Redis<br/>online store)]
        CH[(ClickHouse<br/>offline store)]
        PG[(PostgreSQL)]
    end
    CLIENT([payment source / simulator]) -->|"POST /v1/events"| ING
    DASH([dashboard / scoring client]) --> GW
    GW -->|"REST, timeout+retry"| FEAT
    GW -->|"REST, timeout+retry"| DET
    ING -->|"produce: transactions.raw"| K
    K -->|"consume: transactions.raw"| FEAT
    FEAT -->|"produce: features.updates"| K
    K -->|"consume: features.updates"| DET
    ING --- PG
    FEAT --- R
    FEAT --- CH
    FEAT --- PG
    DET --- PG
```

## Communication

### Asynchronous (Kafka) — the backbone

| Topic | Producer | Consumers | Contract | Retention |
| --- | --- | --- | --- | --- |
| `transactions.raw` | ingestion | features (online + offline writers, independent consumer groups — ADR-001) | `schemas.events.TransactionEvent` v1 | unlimited (replay) |
| `features.updates` | features | detection, dashboard push | `schemas.features.*` | 7 days |

Contracts are versioned (`schema_version` field, frozen models, `extra="forbid"`) and
guarded by round-trip/contract tests in `libs/schemas/tests`. Delivery semantics:
at-least-once with consumer-side idempotency (ADR-003); producers use acks with a
bounded ack timeout (ingestion degrades to `503 + Retry-After` on broker outage,
ADR-006).

### Synchronous (REST)

Only two sync hops exist — `gateway → features` and `gateway → detection` — so no
call chain exceeds 2 services. Every sync call carries: connect/read timeouts,
bounded retries with exponential backoff (idempotent GETs only), and a mapped
`503`/`504` when the dependency is down (graceful degradation: the dashboard shows
"features unavailable", partial results where possible). APIs are versioned under
`/v1/`; OpenAPI specs are exported to [openapi/](openapi/).

### Correlation

`event_id` is the correlation id of the pipeline: assigned at ingestion, carried in
Kafka messages and in `detection.alerts.event_id`. HTTP requests get an
`X-Request-ID` (generated at the gateway, propagated downstream). All services log
single-line JSON with the correlation id field; logs are PAN-masked at the formatter
level.

## Deployment diagram (docker compose)

```mermaid
flowchart TB
    subgraph host [Docker host — infra/docker-compose.yml]
        subgraph net [compose network]
            GW[gateway :8080→8080]
            ING[ingestion :8000→8000]
            FEAT[features :8001]
            DET[detection :8002]
            K[(kafka :29092→9092)]
            PG[(postgres :5432)]
            R[(redis :6379)]
            CH[(clickhouse :8123)]
        end
        SEC[/compose secret: hmac_key/] -.-> ING
        ENV[/.env: DB passwords/] -.-> PG & ING & FEAT & DET & GW
    end
```

- One image per service, built from its own Dockerfile (multi-stage `uv` build,
  non-root `USER`, pinned base images); compose wires healthchecks
  (`condition: service_healthy`) so services start only after their dependencies.
- Every service exposes `GET /health` (liveness, no I/O) and `GET /ready`
  (dependency probes with timeouts).
- Resource limits are set per container; data lives in named volumes.

## Fault-tolerance summary (per dependency)

| Failure | Behaviour |
| --- | --- |
| Kafka down | ingestion: `503 + Retry-After` to clients, no data loss accepted silently (ADR-006); consumers resume from committed offsets |
| Redis down | features: serve `fallback`/`default` statuses per catalog (`FeatureValue.status`), rebuild windows from Kafka replay (ADR-002) |
| Postgres down | ingestion not ready (`/ready` 503); detection buffers via consumer lag |
| features down | gateway returns degraded response after timeout; detection lags, catches up from topic |
| duplicate/replayed events | idempotency store at ingestion; idempotent handlers downstream (ADR-003) |
