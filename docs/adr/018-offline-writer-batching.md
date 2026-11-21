# ADR-018: Offline writer batching and commit

- **Status:** Accepted
- **Date:** 2026-11-21
- **Related:** ADR-001, ADR-004, ADR-007, ADR-014

## Context

The offline-writer (ADR-001) copies `transactions.raw` into ClickHouse `events`
and `features.updates` into `feature_snapshots` (ADR-004). It must not stall the
online path when ClickHouse is slow or down, and it must not lose or invent
offsets when a flush partially succeeds.

## Decision

- Same `features` package and worker image; a separate process runs
  `python -m features.offline` (ADR-007). Consumer group `offline-writer`.
- Buffer records from both topics. Flush when either `OFFLINE_BATCH_ROWS`
  (default 5 000) is reached or `OFFLINE_FLUSH_INTERVAL_S` (default 1 s) has
  elapsed since the first record entered the buffer.
- A flush inserts into both tables (empty sides skipped), then commits the
  highest offset+1 per `(topic, partition)` in the buffer. Nothing is committed
  before a successful insert. A crash between insert and commit redelivers;
  `ReplacingMergeTree` collapses duplicates (ADR-004).
- On ClickHouse failure the buffer is kept, insert is retried with exponential
  backoff from 0.5 s to 10 s, and `getmany` does not run until the flush
  succeeds. Memory is capped at one batch; lag accumulates in Kafka.
- Invalid payloads are skipped with a log and counter, matching the
  online-writer; their offsets still advance on the next successful flush.
- Metrics (ADR-014 patterns): `offline_rows_written_total{table}`,
  `offline_flush_duration_seconds`, `offline_flush_failures_total`,
  `offline_consumer_lag{topic,partition}`, `offline_event_to_store_seconds`
  (flush time minus `ingest_time`, K2 buckets).

## Alternatives considered

- **Commit after each record** — high Kafka and ClickHouse overhead at ingest
  rates the stand already sustains.
- **Dedup inserts in the writer** — duplicates the `ReplacingMergeTree` contract
  and still fails if the process dies after insert.

## Consequences

- ClickHouse outages do not affect Redis freshness (ADR-001).
- Exact counts use `FINAL` or `argMax`; raw row counts may briefly include
  redeliveries until merges run.
- Acceptance targets (p95 offline lag, partition lag in Prometheus) are measured
  against these series after the first simulator run.
