# Criterion: Automated Testing

## Architecture Decision Record

### Status

Status: Accepted · Date: 2026-11-24 · Category: **Automated / quality testing (Python services + React dashboard)**

### Context

The system's core claims are about correctness under failure: window features
must be exact regardless of event order and redelivery (US-2.4), no PAN may
reach any store (NFR-8), the online path must keep serving when ClickHouse is
down, operators may change alerts only according to their role, and alerts must
reach the dashboard within 5 s (US-5.1). Most of these properties live at the
boundary between a service and Kafka, Redis, PostgreSQL or ClickHouse, so
in-process unit tests alone cannot prove them. At the same time the suite has to
stay fast enough to run on every pull request.

### Decision

- **Four layers**, each with its own marker and CI job: unit (no infrastructure,
  every PR), integration (real Kafka / Redis / PostgreSQL / ClickHouse in
  testcontainers, every PR), end-to-end against the full Docker Compose stack
  (nightly and on demand), and UI tests (component tests on every PR, Playwright
  nightly).
- **Real engines for storage logic.** Redis Lua scripts, PostgreSQL partial
  unique indexes, triggers and grants, and ClickHouse `ReplacingMergeTree`
  semantics are tested against the real engine version from `infra/`, not mocks.
  Mocks are used only at process boundaries (HTTP upstreams, clocks, the browser
  network layer).
- **Property-based testing** (Hypothesis) for the one place where examples are
  not enough: window state under arbitrary ordering and duplicates.
- **Quality gates in CI**: 70% line coverage for both stacks, ruff + mypy,
  ESLint + Prettier + `tsc`, dependency audit, generated API types must match the
  OpenAPI spec.

### Alternatives considered

| Alternative | Pros | Cons | Why not chosen |
| --- | --- | --- | --- |
| Mocks / fakes for Redis, PostgreSQL, ClickHouse everywhere | Fast, no Docker | Lua, `ON CONFLICT` on partial indexes, field TTL, `FINAL` behave differently or are unsupported in fakes | Correctness of these is the thesis subject |
| Only e2e on Compose | Closest to production | Slow, hard to localize failures, cannot run on every PR | Kept for full flows; logic is tested lower |
| Example-based tests for window logic | Simple to read | Miss orderings and boundary combinations (hour/minute edges, T−24h, redelivery) | Hypothesis generates them and shrinks failures |
| Single coverage number for the monorepo | One gate | Frontend and backend tools differ | Separate gates: pytest-cov and Vitest v8 |

### Consequences

Positive: storage semantics and failure handling are verified on real engines;
failures are localized by layer. Negative: integration tests need Docker and add
minutes to CI; e2e runs nightly rather than per PR. Neutral: the test pyramid is
wide at the bottom (unit) and narrow at the top (e2e), with many integration
tests in between because the system is storage-heavy.

## Test layers

| Layer | Tools | Size | Location | Runs in CI |
| --- | --- | --- | --- | --- |
| Unit (Python) | pytest, pytest-asyncio, fakeredis, httpx `MockTransport` | 372 tests | `services/*/tests`, `libs/schemas/tests`, `simulator/tests`, `experiments/*/tests`, `tools/pan_scan/tests` | `ci.yml` → `test`, every PR |
| Integration (Python) | pytest + testcontainers (Kafka, Redis 8, PostgreSQL 17, ClickHouse) | 33 tests, marker `integration` | `*_integration.py`, `test_infra.py`, `test_idempotency.py`, `test_publish.py`, `test_replay_property.py`, … | `ci.yml` → `integration`, every PR |
| Property-based | Hypothesis | 1 property, 100 generated cases + 2 fixed `@example`s | `services/features/tests/test_replay_property.py` | `ci.yml` → `integration` |
| End-to-end (Python) | pytest against `make up` | 6 tests, marker `e2e` | `tests/e2e/test_stage*.py` | `e2e.yml`, nightly + manual |
| Component (UI) | Vitest, Testing Library, MSW, jsdom | 72 tests in 28 files | `dashboard/src/**/*.test.ts(x)` | `ci.yml` → `dashboard`, every PR |
| End-to-end (UI) | Playwright, @axe-core/playwright | 3 specs | `dashboard/e2e/*.spec.ts` | `e2e.yml` → `dashboard`, nightly |
| Load | k6 | smoke in CI; ladders as experiments | `experiments/load/features.js`, `experiments/results/` | `e2e.yml` (k6 smoke) |

Coverage (latest local run on `main`):

| Stack | Tool | Lines | Gate |
| --- | --- | --- | --- |
| Python services, libs, simulator, baseline | pytest-cov | 75.4% | `--cov-fail-under=70` (`pyproject.toml`) |
| Dashboard `src/` | Vitest v8 | 81.1% | `thresholds.lines: 70` (`dashboard/vite.config.ts`) |

The CI logs print the per-file table for both; the dashboard writes `lcov` for
an HTML report.

## Naming and structure

- Python: tests live next to the package they test, in `<package>/tests/`,
  files `test_<module>.py`; integration files end with `_integration.py` or are
  marked `pytestmark = pytest.mark.integration`; full-stack scenarios live in
  `tests/e2e/test_stage<N>_<scenario>.py`, marked `e2e`.
- Shared fixtures: `conftest.py` per service; reusable stacks in
  `services/features/tests/stack.py`, `services/detection/tests/detection_stack.py`,
  `services/ingestion/tests/containers.py`, `services/gateway/tests/harness.py`.
- Dashboard: `*.test.ts(x)` next to the code it tests (feature-sliced layout);
  MSW handlers and setup in `dashboard/src/test/`; Playwright specs and helpers
  in `dashboard/e2e/`.
- Test names state behaviour, not implementation:
  `test_same_event_id_new_offset_is_dup_event`,
  `test_stage4_clickhouse_outage_keeps_features_fresh`,
  `viewer has no actions and sees the stream recover`.

## Running tests locally

```bash
# Python: unit (no Docker needed)
uv sync
make test                      # pytest -m "not integration and not e2e" with coverage

# Python: integration (Docker required; testcontainers start the engines)
make test-int                  # pytest -m integration (CI adds --no-cov: the 70% gate applies to the unit run)

# Full stack: e2e (Docker Compose)
make secret up topics baseline-up  # once per clean checkout
make test-e2e                      # pytest -m e2e against the running stack

# Dashboard
cd dashboard
npm ci
npm run test                   # Vitest + coverage
npx playwright install chromium
npm run test:e2e               # needs the compose stack on localhost:8081

# Load (optional)
uv run python experiments/load/sample_tokens.py --n 50
SMOKE=1 k6 run -e TARGET=direct -e HOST=localhost experiments/load/features.js
```

Static checks run with the same commands as CI: `make lint typecheck audit`,
`npm run lint`, `npm run format:check`, `npm run typecheck`.

## What is covered: business-critical areas

| Area | What is verified | Where |
| --- | --- | --- |
| Input validation | event schema and PAN/Luhn checks; 422 with field errors; 24 h acceptance window; lookup form rejects a PAN instead of a token | `libs/schemas/tests/test_contracts.py`, `services/ingestion/tests/test_api.py`, `test_errors.py`, `dashboard/src/features/feature-lookup/model/schema.test.ts` |
| PAN never stored | HMAC tokenization; masking in logs; scanner over Kafka, PostgreSQL, Redis, ClickHouse and logs: planted PANs found and masked, epoch-ms numbers and hex tokens not flagged, unknown Redis keys scanned whole | `services/ingestion/tests/test_tokenize.py`, `test_logging.py`, `tools/pan_scan/tests/` |
| Exactly-once acceptance | idempotency key, duplicate `payment_id` returns the same event, Kafka outage → 503 + `Retry-After` (ADR-006) | `services/ingestion/tests/test_idempotency.py`, `test_publish.py` |
| Window correctness | card windows (T − w, T], merchant two-level buckets, field and key expiry from `event_time`, late events, redelivery and `dup_event` | `services/features/tests/test_windows.py`, `test_apply.py`, `test_replay_property.py` |
| Freshness semantics | `fresh` / `stale` / `default` by watermark and heartbeat (ADR-013) | `services/features/tests/test_freshness.py`, `tests/e2e/test_stage3_features.py` |
| Offline copy | batch flush by size and time, no commit before insert, catch-up after ClickHouse pause and after writer crash, `FINAL` row count = unique `event_id` | `services/features/tests/test_offline.py`, `test_offline_recovery.py`, `tests/e2e/test_stage4_offline.py` |
| Error handling | one Problem Details format in every service; unexpected exception → 500 with `request_id` and no PAN in body or log | `*/tests/test_errors.py`, `libs/schemas/tests/test_errors.py` |
| Permissions | JWT issue/verify, expired, wrong key, `alg: none`; RBAC matrix; CSRF header; same message for unknown email and wrong password; client-supplied `X-Operator-*` never reaches upstream; DB grants of `svc_gateway` | `services/gateway/tests/test_auth.py`, `test_proxy.py`, `test_auth_integration.py` |
| Alert lifecycle | transition × role matrix, keyset pagination without gaps or repeats, two concurrent `PATCH` → one 200 and one 409, audit entry with operator and note | `services/detection/tests/test_api.py`, `test_alerts_api_integration.py` |
| Live stream | NOTIFY after commit only, rolled back insert → no event, `Last-Event-ID` replay, slow subscriber dropped, stats windows | `services/detection/tests/test_stream_integration.py`, `test_live_broadcast.py`, `test_live_stats.py` |
| UI behaviour | 401/403/404/409/422/429/5xx reactions, session bootstrap, SSE parsing and backoff, push → cache, optimistic update with rollback, stale banner | `dashboard/src/shared/api/*.test.ts`, `features/stream/model/*.test.ts`, `features/session/**/*.test.ts(x)` |
| Key user flows | analyst: alert appears without reload → take → resolve; viewer: no actions, banner on paused gateway and recovery; accessibility | `dashboard/e2e/analyst.spec.ts`, `viewer.spec.ts`, `a11y.spec.ts` |

## Techniques

- **In-memory and containerized infrastructure.** fakeredis with Lua for fast
  unit tests of window code; testcontainers for real Redis 8 (field TTL), Kafka,
  PostgreSQL 17 (init scripts from `infra/postgres/` are applied, so roles,
  triggers and partial indexes are tested as deployed) and ClickHouse.
- **Test doubles at boundaries.** httpx `MockTransport` for gateway upstreams;
  injected clocks (`now_ms`, `sleep`, `random`) for flush timers, backoff and
  statistics windows; a fake `fetch` stream for the SSE client; MSW for the
  dashboard API.
- **Fixtures.** Session-scoped containers with per-test cleanup (`FLUSHDB`,
  fresh topics), so tests stay independent and repeatable (FIRST).
- **Property-based testing.** `test_replay_property.py` generates 1–60 events
  over three cards and two merchants, event times in [T − 26 h, T] with a
  separate strategy aimed at boundaries (T − 1 min, T − 1 h, T − 24 h ± 1 ms,
  minute and hour edges), shuffles delivery and injects both kinds of repeat:
  the same Kafka record and the same `event_id` at a new offset. Every stream
  feature must equal an independent pure-Python oracle written from ADR-005.
- **Contract checks.** Message and API contracts are Pydantic models in
  `libs/schemas` with round-trip tests; dashboard types are generated from the
  gateway OpenAPI and CI fails if they drift; SSE payloads are validated at
  runtime with zod.
- **Waiting without sleeps.** Integration and e2e tests poll for a condition
  with a deadline ("until lag = 0, at most 60 s") instead of fixed pauses, so
  timing on a slow CI runner does not cause flakiness.

## Complex scenarios

| Scenario | Test |
| --- | --- |
| Multi-service flow ingestion → Kafka → online writer → Feature API | `tests/e2e/test_stage2.py`, `test_stage3_features.py` |
| Online vs SQL baseline give the same stream features on one stream | `tests/e2e/test_stage3_features.py::test_baseline_stream_features_match_online` |
| Backfill of history through the real ingestion window | `tests/e2e/test_stage3_backfill.py` |
| ClickHouse outage under traffic: Redis stays fresh, offline catches up with no loss or duplicates | `tests/e2e/test_stage4_offline.py`, `services/features/tests/test_offline_recovery.py` |
| Component failure visible in the system stream within 10 s (US-6.2) | `tests/e2e/test_stage7_system_stream.py` |
| Asynchronous workflows and message queues | consumer and publisher tests on real Kafka in `services/*/tests/*integration*.py` |
| Database "migrations" | schema is created only by versioned init scripts; `libs/schemas/tests/test_infra.py` applies them to a clean PostgreSQL and ClickHouse and checks roles and grants |

## CI pipeline and quality gates

| Job | Steps | Trigger |
| --- | --- | --- |
| `lint` | ruff, mypy (strict), `pip-audit` on `uv export --frozen`, `promtool` for Prometheus config | every PR |
| `test` | unit tests with coverage gate 70% | every PR |
| `integration` | testcontainers suite incl. Hypothesis property | every PR |
| `dashboard` | ESLint, Prettier, `tsc`, generated types current, Vitest with coverage gate 70%, build, `npm audit --audit-level=high`, image build | every PR |
| `e2e` → `stage2` | `make secret up topics baseline-up test-e2e`, k6 smoke | nightly 03:00 UTC, manual |
| `e2e` → `dashboard` | Playwright against the stack, traces uploaded on failure | after `stage2` |

## Defects found by tests

| What happened | Found by | Fix |
| --- | --- | --- |
| First K1/K2 run: 98.8% errors and the online writer stalled at 200 RPS | k6 load run + metrics; the run was marked INVALID instead of reported | Read-path ladders located the bottleneck in merchant window reads (1 440 HGETs per request); two-level buckets and Lua reads (ADR-016) |
| A duplicate `event_id` at a new Kafka offset counted twice in merchant windows (card windows were already safe) | Hypothesis property; the fixed `@example` keeps the counterexample in the suite | Event-level dedup via the card `ZADD` result, outcome `dup_event`, no snapshot republished (ADR-017) |
| Merchant hash expired as a whole 25 h after creation, zeroing an active merchant | review of the write path; covered by regression tests on field expiry | Field-level `HPEXPIREAT` from `event_time`; key expiry for `card:` and `geo:` (ADR-017); `test_apply.py::test_stream_window_count_matches_last_1440_minutes`, `test_field_and_key_expiry_from_event_time` |

## Known gaps and justification

- **Fallback and rebuild are not tested** because they are not implemented yet
  (stages 5–6: degradation and recovery). Their acceptance tests are specified
  in the design document and will be added with the code.
- **`impossible_travel` rule** is disabled (on-demand features are not in the
  snapshot, ADR-013), so it has no alert-path test.
- **e2e runs nightly, not on every PR**: a full Compose build plus two minutes
  of traffic takes about 20–30 minutes. Logic covered by e2e is also covered by
  unit and integration tests, which do run per PR.
- **No mutation testing and no consumer-driven contract tests** (Pact).
  Contracts are shared Pydantic models plus generated client types, which covers
  drift between our own services; external consumers do not exist yet.
- **Dashboard `shared/ui`** (skeleton and empty-state components) is at 58% line
  coverage; these are presentational and are exercised by page tests and e2e.

## Recommendations

1. Add stage 5–6 tests: Redis stop under load → `fallback` with correct values
   (K3), flush + rebuild → 100% parity (K4, K5).
2. Run mutation testing (`mutmut`) on `features/windows.py` and `apply.lua`
   wrappers — the code where a wrong boundary is most costly.
3. Add contract tests for the SSE events against `docs/asyncapi/*.yaml`.
4. Raise the coverage gate to 80% once stages 5–6 land.
5. Publish CI status badges in the README and keep the nightly e2e history as
   stability evidence.

## Requirements checklist (rubric: Automated Testing)

### Minimum

| # | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Structured, documented setup validating core logic | ✅ | this document; [What is covered](#what-is-covered-business-critical-areas) |
| 2 | ≥ 70% coverage with numbers and a report | ✅ | 75.4% Python, 81.1% dashboard; gates in `pyproject.toml`, `vite.config.ts`; tables in CI logs |
| 3 | Unit tests, FIRST, common and edge cases | ✅ | 372 Python + 72 dashboard tests; injected clocks, no shared state |
| 4 | Integration tests: API, DB, auth, configuration | ✅ | 33 integration tests on real engines |
| 5 | UI tests: component and/or e2e | ✅ | Vitest + Testing Library; 3 Playwright specs |
| 6 | All tests in CI, results visible in logs | ✅ | `ci.yml` per PR; `e2e.yml` nightly |
| 7 | Strategy in the report: types, tools, local run, gaps | ✅ | [Test layers](#test-layers), [Running tests locally](#running-tests-locally), [Known gaps](#known-gaps-and-justification) |
| 8 | Naming convention and directory structure | ✅ | [Naming and structure](#naming-and-structure) |

### Maximum

| Requirement | Status | Evidence |
| --- | --- | --- |
| Unit, integration, component and e2e layers | ✅ | [Test layers](#test-layers) |
| Business-critical areas: validation, errors, permissions, data processing | ✅ | [What is covered](#what-is-covered-business-critical-areas) |
| Mocks, test doubles, fixtures, in-memory infrastructure | ✅ | [Techniques](#techniques) |
| Multi-service, async, message queues, DB scripts, full e2e | ✅ | [Complex scenarios](#complex-scenarios) |
| Additional methodology (property-based, contract, …) | ✅ | Hypothesis; OpenAPI-generated types checked in CI |
| Full CI with quality gates: coverage, lint, static analysis | ✅ | [CI pipeline](#ci-pipeline-and-quality-gates) |
| Evidence of stable execution (screenshots or badges) | ⚠️ | add CI badges to README and screenshots of green runs |
| Rationale, defects found, recommendations | ✅ | ADR, [Defects found](#defects-found-by-tests), [Recommendations](#recommendations) |
| Performance / load testing | ✅ | k6 scenario and ladders in `experiments/` |
