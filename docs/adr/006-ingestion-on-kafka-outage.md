# ADR-006: Ingestion behaviour when Kafka is unavailable

- **Status:** Accepted
- **Date:** 2026-11-14
- **Related:** NFR-4, NFR-5, K3; ADR-003

## Context

When Kafka is down, the ingestion service cannot publish accepted events. The BA does
not define what the source receives in that case. Serving keeps working from Redis
(values become stale), but new events must not be silently lost.

## Decision

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
- Known gap: if Kafka acknowledges the message and the following `COMMIT` fails, the
  idempotency row is rolled back. A source retry inserts the key again and publishes
  a second message at a new offset. Producer idempotence does not collapse that
  retry, because it is a new send. The same gap exists when the application ack
  timeout fires and a late acknowledgement still appends the record. Downstream
  deduplication (ADR-003) has to tolerate that duplicate.
