# ADR-006: Ingestion behaviour when Kafka is unavailable

- **Status:** Proposed (decide before Stage 5)
- **Date:** 2026-11-07
- **Related:** NFR-4, NFR-5, K3; ADR-003

## Context

When Kafka is down, the ingestion service cannot publish accepted events. The BA does
not define what the source receives in that case. Serving keeps working from Redis
(values become stale), but new events must not be silently lost.

## Decision (default proposal)

Ingestion returns **503 Service Unavailable** with `Retry-After`, does not record the
`event_id` in the idempotency table, and the source retries. The idempotency row is
written in the same transaction that is committed only after Kafka acknowledges the
message.

## Alternatives considered

- **Local buffer in ingestion** (disk queue, replayed when Kafka is back) — fewer
  rejected requests, but a second log with its own durability and ordering problems.

## Consequences

- No event is accepted without being in Kafka; nothing is lost silently.
- The source must implement retries; the simulator does.
- To be confirmed after the first failure experiments in Stage 5.
