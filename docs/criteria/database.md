# Criterion: Database

## Architecture Decision Record

### Status

Status: Accepted · Date: 2026-11-15 · Category: **Transactional (OLTP), PostgreSQL**

### Context

The stream pipeline cannot hold durable, relational state: the idempotency window
for exactly-once acceptance (ADR-003), the registry of feature definitions and
versions that alert rules reference, the rules with their per-feature comparison
clauses, the alert lifecycle (open → acknowledged → resolved) with an audit
trail, operator accounts, and the merchant registry. This state is relational by
nature (rules ↔ features are M:N, alerts reference rules and merchants), needs
strict integrity (an alert must point to an existing rule; a resolved alert must
carry its resolution time; at most one open alert per rule and subject) and
transactional updates (status transition + audit atomically).

### Decision

PostgreSQL 17 as the OLTP system of record, one schema per bounded context
(`ingestion`, `ref`, `features`, `detection`, `ops`), per-service least-privilege
accounts created from the environment (no credentials in Git), structure created
only from versioned scripts in `infra/postgres/`. Uniqueness guarantees that the
pipeline depends on (one open alert per subject) live in the schema as partial
unique indexes, not in consumer code. Two further engines serve workloads
PostgreSQL is not shaped for — Redis as the online feature store (ms-level reads,
ADR-002) and ClickHouse as the offline store (append-heavy history, ADR-004) —
with PostgreSQL remaining the system of record for everything above. The
`baseline` schema is a measurement stand (ADR-015), not a product bounded
context.

### Alternatives considered

| Alternative | Pros | Cons | Why not chosen |
| --- | --- | --- | --- |
| MongoDB for rules/alerts | Flexible shape | No FKs/CHECKs across rules↔features↔alerts; partial unique "one open per subject" not expressible as declaratively | Integrity is the core requirement here |
| Keep all state in Redis/ClickHouse | Fewer engines | No transactional lifecycle, no constraints; system-of-record mixed into caches/analytics | Durability and integrity expectations fail |
| SQLite | Zero ops | Single writer, no roles | Four services connect concurrently under distinct accounts |
| One `public` schema, one shared DB user | Simpler | Any service could write anything; violates the microservices no-shared-DB rule | Least privilege by schema is cheap and auditable |
| Dedup alerts in consumer code (`WHERE NOT EXISTS` only) | No index needed | Racy under concurrent consumers | Partial unique indexes make the guarantee schema-level |

### Consequences

Positive: constraints enforce the invariants the thesis relies on; services are
isolated by schema + grants; the database rebuilds deterministically from Git.
Negative: three engines raise operational surface — mitigated by compose
healthchecks and by scoping Redis/ClickHouse to the features service. Neutral:
the feature registry duplicates the code catalog (ADR-012) as a read model; the
startup upsert keeps them consistent and versions accumulate as history.

## Implementation

- Roles (env-based passwords): `infra/postgres/00-roles.sh`; ClickHouse
  service users: `infra/clickhouse/00-users.sh`
- DDL: `infra/postgres/10-schema.sql`, `40-alerts-ui.sql` · Seed: `infra/postgres/20-seed.sql`;
  ClickHouse offline DDL + grants: `infra/clickhouse/10-schema.sql`
- Baseline stand (not a product schema): `infra/postgres/30-baseline.sql`, role `svc_baseline`
- Data dictionary, ER diagram, integrity, versioning:
  [`docs/db/data-dictionary.md`](../db/data-dictionary.md)
- Consumers of the schema: `services/detection` (rules, alerts, audit via
  trigger), `services/features` (registry upsert), `services/gateway`
  (read-only catalog and alerts), `services/ingestion` (idempotency).

## Requirements checklist (rubric: Transactional Databases, minimum)

| # | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Data dictionary | ✅ | docs/db/data-dictionary.md |
| 2 | Integrity and transaction handling described | ✅ | data-dictionary.md §Integrity; ADR-003 |
| 3 | Logical schema / ER diagram | ✅ | data-dictionary.md §ER |
| 4 | DDL script | ✅ | infra/postgres/10-schema.sql, 30-baseline.sql |
| 5 | Modern relational DBMS | ✅ | PostgreSQL 17 |
| 6 | 3NF, enough tables, different relationship types | ✅ | 9 tables; 1:N and clause-level M:N |
| 7 | PK/FK, CHECKs, meaningful types everywhere | ✅ | 10-schema.sql (incl. XOR subject check, status⇔resolved_at) |
| 8 | Structure only via scripts, versioned in Git | ✅ | infra/postgres/, reviewed PRs |
| 9 | Test data incl. edge cases; reference fill scripts | ✅ | 20-seed.sql; seeded simulator (bursts, declines, geo) as the stream generator |
| 10 | Roles read/write/admin; app not under superuser | ✅ | Postgres: 00-roles.sh (fs_* tiers, svc_* accounts), grants in 10-schema.sql; ClickHouse: 00-users.sh (svc_offline, svc_features_read), grants in clickhouse/10-schema.sql |
| 11 | Permissions kept in review when objects change | ✅ | Grants live in the same reviewed scripts |
| 12 | Passwords only hashed, modern algorithm | ✅ | bcrypt (pgcrypto); format CHECK rejects plain text |
| 13 | No CSV-as-storage / unjustified JSON | ✅ | Single jsonb column, justified in data-dictionary.md |
| 14 | No ignored integrity | ✅ | FKs + CHECKs + partial unique indexes |

### Maximum-level items implemented

| Item | Status | Evidence |
| --- | --- | --- |
| Different DBMS types in one project, justified | ✅ | PostgreSQL (OLTP) + Redis (online store, ADR-002) + ClickHouse (offline, ADR-004) |
| Indexes implemented and justified | ✅ | Partial unique "one open per subject" ×2, history index; rationale in data-dictionary.md |
| Triggers / functions with business logic | ✅ | SECURITY DEFINER audit trigger; idempotency cleanup function |
| Views | ✅ | detection.open_alerts_v, ops.operators_masked |
| Masking / anonymization of personal data | ✅ | operators_masked view; PANs never stored — HMAC tokens by design (ADR-009) |
| Schema versioning described | ✅ | data-dictionary.md §Schema versioning |
| Data layers raw→staging→mart | ⚠️ | Belongs to the ClickHouse offline stage (ADR-004), outside OLTP scope |

## Known limitations

| Limitation | Impact | Plan |
| --- | --- | --- |
| Init scripts instead of a migration tool | None pre-release; history = Git | Numbered `NN-*.sql` (or Alembic) at first post-release change |
| Alert actor columns are text, not FKs to operators | Actors are gateway session ids (ADR-020), not mandatory FKs in MVP | FK once operators are stable across environments |
| Registry briefly trails the code catalog at deploy | Read model only | Startup upsert is idempotent; stores stay authoritative |
