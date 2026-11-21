# ADR-021: Live stream for the dashboard

- **Status:** Accepted
- **Date:** 2026-11-21
- **Related:** US-5.1, NFR-3, ADR-003, ADR-008, ADR-014, ADR-019, ADR-020

## Context

Operators need alerts within 5 s of the triggering event and a live overview of
throughput. Client polling is forbidden by the Real-time Analytics criterion.
SSE gives one-way push, automatic reconnect, and `Last-Event-ID` over plain HTTP.

## Decision

- **Alert source:** PostgreSQL `LISTEN/NOTIFY` on `detection.alerts` after
  `INSERT` or `UPDATE OF status`. NOTIFY runs only after commit, so rolled-back
  inserts never appear. Status changes from other operators are pushed the same way.
- **Stats source:** the detection consumer's `features.updates` stream. A sliding
  60 s window of 1 s buckets lives in `detection/live/stats.py`. Prometheus is not
  polled for live TPS (that would be scheduled fetch, not an event stream).
- **Fan-out:** in-process in detection. Each subscriber has a bounded queue (100);
  overflow drops the slow client. Cap 200 subscribers → HTTP 503. A ring buffer of
  the last 500 `alert` events supports resume via `Last-Event-ID`; outside the
  buffer (or after process restart) the stream sends `reset` and the client
  reloads via REST.
- **Masking:** stream payloads abbreviate `card_token` (`ab12…ef`). Full tokens
  remain on the REST alert card only.
- **Gateway:** a dedicated streaming proxy path: no read timeout, no retries,
  forwards `Last-Event-ID`. This is an exception to the ADR-008 REST timeouts.
  The stream ends at JWT `exp` (ADR-020); the client reconnects and gets 401 if
  the session is gone. Rate limits apply to opening the connection, not to SSE
  chunks.
- **Multi-replica caveat:** every replica receives NOTIFY, so alerts stay
  correct. Stats on each replica see only that replica's Kafka partitions. A
  dedicated stats consumer group is future work.

## Alternatives considered

- **In-memory queue from the consumer** — misses alerts written by PATCH and
  surfaces rolled-back inserts if published before commit.
- **WebSocket** — bidirectional without need; harder through HTTP proxies.
- **Polling Prometheus for TPS** — violates the push-only live-data rule.

## Consequences

- DDL: notify function/trigger in `infra/postgres/40-alerts-ui.sql`.
- Contract: `GET /v1/stream/alerts` (`text/event-stream`); schemas in
  `libs/schemas` and a short AsyncAPI note. OpenAPI does not describe streams.
- Metrics: `detection_stream_subscribers`, `detection_stream_dropped_total`,
  `detection_stream_delivery_seconds`.
