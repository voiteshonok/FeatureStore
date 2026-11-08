# ADR-003: Delivery semantics and deduplication

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** US-1.3, NFR-5, K4; ADR-002

## Context

Kafka consumers can receive a message again after a restart or rebalance. Card windows
deduplicate by `event_id` (ADR-002), but merchant minute buckets store only numbers and
cannot recognise a repeated event. The ingestion producer can also publish a message
twice when a send is retried after a timeout.

## Decision

- **At-least-once** consumption; offsets are committed after a successful write.
- **Layer 1 — producer:** the ingestion Kafka producer runs with
  `enable_idempotence=True`, so producer retries do not create duplicate messages.
  Source-level duplicates (same `event_id` sent twice by the source) are stopped earlier
  by the idempotency table in PostgreSQL.
- **Layer 2 — offset watermark:** the online-writer applies each event through one
  Lua script that atomically updates the card set, the merchant bucket and the
  per-partition offset watermark. An event whose offset is not above the watermark of
  its partition is skipped.
- The offline-writer deduplicates by `event_id` in ClickHouse.

## Alternatives considered

- **Exactly-once via Kafka transactions** — does not cover writes to Redis, which is
  outside Kafka.
- **A seen-`event_id` set in Redis with a 24 h TTL** — about 17 million extra keys,
  more than 1 GB of memory.

## Consequences

- Redelivery after a restart is harmless, which directly supports NFR-5.
- The Lua script is the single write path to Redis and must stay small and tested.
- A rebuild (K5) resets the watermarks before replaying the log from the beginning.
