# ADR-001: Independent online and offline writers

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** K2, K3, NFR-4, NFR-5, NFR-11; ADR-003, ADR-004, ADR-007

## Context

Both stores are filled from the event log (dual-store pattern, BA section 1.2.2).
If one consumer wrote to both Redis and ClickHouse, a ClickHouse outage or slow batch
would stall Redis updates and break freshness (K2) and serving under failure (K3).
Feature logic must still be implemented only once (G3, P3).

## Decision

- The feature computation logic is a single Python module: a pure function
  "state + event → feature values".
- **online-writer** (consumer group `online-writer`) reads `transactions.raw`, applies
  the logic to Redis state and publishes the resulting feature snapshot (feature values
  after the event, with `event_time` and `event_id`) to `features.updates`.
- **offline-writer** (consumer group `offline-writer`) reads `transactions.raw` into the
  ClickHouse `events` table and `features.updates` into `feature_snapshots`, in batches.
- The two groups commit offsets independently; each catches up from its own offset
  after an outage.

## Alternatives considered

- **One consumer writing both stores** — couples the stores' availability; violates K3.
- **offline-writer recomputes windows itself** — a second implementation of every
  feature, i.e. the training-serving skew the project removes (P3).

## Consequences

- A ClickHouse outage does not affect Redis freshness; a Redis outage stops new
  snapshots, so the fallback serves the last known snapshot with its `as_of`.
- Training data contains exactly the values that were served.
- Detection reads `features.updates` and needs no access to Redis.
- Two topics must be kept in sync conceptually; parity (ADR-005) verifies the result.
