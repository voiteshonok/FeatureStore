# ADR-004: Offline storage layout in ClickHouse

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** US-3.3, US-4.1, US-4.2, K3, K4; ADR-001, ADR-005

## Context

The offline store serves three purposes: last known values for the fallback when
Redis is down (US-3.3), point-in-time training exports (US-4.1, US-4.2), and an
independent reference for the parity check (K4). Feature logic must exist only once
(ADR-001).

## Decision

Two tables, both `MergeTree`-based, written in batches by the offline-writer:

- **`events`** — raw transaction events from `transactions.raw`.
  `ReplacingMergeTree`, `ORDER BY (card_token, event_time, event_id)`.
- **`feature_snapshots`** — feature values after each event from `features.updates`.
  `ReplacingMergeTree`, `ORDER BY (entity_type, entity_id, event_time, event_id)`.

Usage:

- **Fallback:** latest snapshot per entity via `argMax(..., event_time)`.
- **Training export:** `ASOF JOIN` of (entity, timestamp) pairs to `feature_snapshots`,
  so the model trains on the values that were actually served.
- **Parity:** window values recomputed by SQL over `events`, with the same window
  definitions as Redis. This SQL is a test oracle only, never a serving path.

## Alternatives considered

- **Raw events only, features computed by SQL for export** — a second implementation
  of every feature (P3).
- **Snapshots only** — no independent data to verify parity against.

## Consequences

- Fallback values may be older when Redis has been down for a while; `as_of` makes
  their age explicit.
- Deduplication in `ReplacingMergeTree` is eventual; queries that need exact results
  use `FINAL` or `argMax`.
- Storage grows with every event; retention is not a concern for the experiment scale.
