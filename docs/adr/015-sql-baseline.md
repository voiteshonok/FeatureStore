# ADR-015: SQL baseline

- **Status:** Accepted
- **Date:** 2026-11-16
- **Related:** K1, K2; ADR-002, ADR-005, ADR-014

## Context

K1 compares feature-read latency with an AS-IS path that aggregates the same
windows from PostgreSQL at request time. That path is a measurement stand. A
stand that is slow because indexes are missing, each request opens a
connection, or the memory limit pushes a day of events to disk does not measure
serving.

A day at the ADR-002 rate is about 17.3 million events. `card_token` is a
64-character HMAC. The heap, the primary key and the two read indexes are about
7 GB together. The previous 1 GB Postgres limit cannot hold that working set.
3 GB, the Redis limit, cannot hold it either.

## Decision

- The stand lives in `experiments/baseline`, a uv workspace package, and in the
  `baseline` compose profile (`baseline-writer`, `baseline-api`). It is not a
  product service: it is not a root dependency, not behind the gateway, and not
  in the microservice list.
- It does not import `services/features`. Window bounds and haversine are
  duplicated. Contracts come from `libs/schemas`.
- `baseline-api` serves `POST /v1/features` on port 8003 with `FeatureRequest`
  and `FeatureResponse`. T is the API server clock, as in the feature API.
  `request.ts` is only the timestamp of the request coordinates for
  geo-velocity.
- Card windows are `(T − window, T]` on `event_time`. Merchant windows are the
  last 1,440 UTC minute buckets, including the current one:
  `[minute(T) − 1439, minute(T)]`.
- Reads use the indexes `(card_token, event_time DESC)` and
  `(merchant_id, event_time DESC)`, an API connection pool (min 4, max 16), and
  psycopg prepared statements (`prepare_threshold=0` on the extended protocol).
  Each pooled connection prepares the three read statements when the pool
  creates it.
- The writer uses consumer group `baseline-writer` on `transactions.raw`. It
  inserts batches of up to 200 records or 50 ms with
  `INSERT … ON CONFLICT (event_id) DO NOTHING` and commits offsets only after
  the database transaction commits. The writer holds one connection. An invalid
  record is logged and skipped; its offset is still committed.
- Statuses are `fresh` when the entity has rows in its 24 h window (card: the
  24 h count; merchant: the 1,440 buckets), otherwise `default` with the
  catalog value. The stand does not return `stale` or `fallback`. A fresh
  stream value has `as_of = T` and `age_ms = 0`.
- There is no retention and no partitioning.
- Metrics follow ADR-014, with the same histogram buckets:
  `baseline_request_duration_seconds`, `baseline_event_to_store_seconds`
  (batch-commit time minus `ingest_time`), `baseline_consumer_lag`. Each
  process serves only the series it records.
- Postgres run configuration for the experiment, on the shared instance:
  container memory `8g`, `/dev/shm` `1gb`, `shared_buffers=1GB`,
  `effective_cache_size=6GB`. `ANALYZE baseline.transactions` after a warm-up
  replay is an operator step. `svc_baseline` is not the table owner and cannot
  run it.

## Alternatives considered

- **3 GB, equal to Redis** — the day does not fit, so the stand would measure
  disk.
- **A second Postgres service** — a different cache from the instance whose
  init scripts create the stand.
- **Routing the stand through the gateway** — gateway time would be mixed into
  the AS-IS latency.

## Consequences

- `make up` does not start the stand. `make baseline-up` does.
- Init scripts run only when the Postgres volume is created. Applying this
  schema to an existing volume requires `make clean && make up`.
- The 8 GB limit is a cgroup ceiling on the Postgres the product uses. A
  measurement run needs the host to actually provide that RAM.
- A caught-up partition reports lag 0.
