# ADR-022: System state stream for the dashboard

- **Status:** Accepted
- **Date:** 2026-11-21
- **Related:** US-6.2, ADR-008, ADR-014, ADR-020, ADR-021

## Context

Operators need to see a stopped component within 10 s without client polling
(Real-time Analytics forbids scheduled refresh from the browser). Component
health is not an event stream: it is the result of dependency probes. After
stage 5 the same channel will carry a `fallback` degradation mode.

## Decision

- **Aggregation in the gateway** (API composition / BFF). A single background
  collector probes each HTTP service's `GET /ready` every 2 s (1 s timeout,
  dedicated client, not the proxy pool) and fans out to all SSE subscribers.
  Subscriber count must not multiply probe traffic.
- **Workers without HTTP** (`features-worker`, `features-offline`) are read from
  Prometheus `up` and consumer-lag gauges (`features_consumer_lag`,
  `offline_consumer_lag`) every 5 s. Prometheus moves into the default compose
  stack (no longer `obs`-only).
- **Infra status** (Kafka, Redis, PostgreSQL) is taken from `/ready` JSON bodies
  of services that already probe those stores. The gateway never opens
  connections to foreign stores. ClickHouse has no `/ready` reporter today →
  status stays `unknown` until a probe exists without direct gateway access.
- **SSE** `GET /v1/stream/system`: event `snapshot` on connect, event `change`
  only when a status flips or lag crosses 1000. Field `mode` is `normal` or
  `degraded` (any `down`); `fallback` is reserved for stage 5. Session operators
  only (same RBAC as alert reads). `: ping` every 15 s.
- **Partial results:** if Prometheus is down, `lag` is `null` and HTTP-derived
  statuses still flow. This is intentional Microservices partial availability.

Server-side polling does not violate the Real-time criterion: the criterion
forbids the *client* refreshing on a timer. Live TPS and alerts remain a push
stream from ADR-021; system state is composed once for all dashboards and
pushed only on change.

## Alternatives considered

- **Proxy each dashboard to poll `/ready`** — probe load scales with open tabs.
- **WebSocket** — bidirectional without need; harder through HTTP proxies.
- **Gateway connecting to Redis/Kafka/ClickHouse** — couples the edge to stores
  and duplicates probes already owned by services.

## Consequences

- Contract: `GET /v1/stream/system` (`text/event-stream`); schemas in
  `libs/schemas.stream`; AsyncAPI note under `docs/asyncapi/`.
- Settings: `INGESTION_URL`, `PROMETHEUS_URL` on the gateway.
- `make up` starts Prometheus; README and architecture docs drop the
  obs-only framing for the system stream.
