# ADR-005: Semantics of online/offline parity

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** K4, NFR-7, US-4.4, G3; ADR-002, ADR-004

## Context

K4 requires at least 99.9% of sampled feature values to be identical in the online and
offline stores. Without a precise definition of what is compared, the KPI cannot be
measured. Redis keeps only the current state, while ClickHouse keeps history.

## Decision

- **Feature values are compared, not raw data.** For entity E and moment T, the value
  of feature F computed from Redis must equal F computed from ClickHouse `events` with
  the same window definition: range (T − window, T], and the same minute buckets for
  merchants.
- **Main measurement — frozen snapshot:** stop the simulator, wait until the lag of both
  writers is 0, sample 10,000 random cards and merchants, set T = now, compare all
  stream features of the catalog.
- **Measurement under load:** an event with `ingest_time` later than T − 2 s (the K2
  threshold) is not counted as a mismatch.
- On-demand features (geo-velocity) are not compared.

## Alternatives considered

- **Comparing raw event sets** — simpler, but does not detect differences in window
  formulas.

## Consequences

- The check verifies both delivery (lost, duplicate or reordered events) and window
  definitions, which is what a model actually sees.
- T is limited to the present, because Redis has no history.
- The ClickHouse SQL for windows must be kept equivalent to the Python logic; a
  mismatch shows up as a parity failure.
