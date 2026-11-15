# ADR-013: Freshness semantics

- **Status:** Accepted
- **Date:** 2026-11-15
- **Related:** US-3.2; ADR-000, ADR-001, ADR-003, ADR-012

## Context

The feature API has to say how old a served value is (US-3.2). `FeatureValue`
already carries `as_of`, `age_ms` and a status, and the catalog already sets
`stale_after` on each stream feature (ADR-012): 5 s for card features, 30 s for
merchant features. Nothing yet defines what those fields mean, or the message
the online-writer publishes to `features.updates` (ADR-001).

`transactions.raw` is keyed by `card_token`, so every event for one card lands
on one partition. A merchant's events are spread across partitions.

## Decision

The online-writer publishes a `FeatureSnapshot` after it applies an event. The
payload is JSON from the Pydantic model, with `schema_version`. Fields are
`entity_type` (`card` or `merchant`), `entity_id`, `event_id`, `event_time`,
`ingest_time`, `features`, and `source_partition` / `source_offset` of the
source record on `transactions.raw`. Feature names must be stream features of
that entity in the catalog; values are `int` or `float`. Unknown names,
on-demand names and names for the other entity are rejected. The writer
publishes every stream feature of the entity. The schema accepts any subset of
those names.

`as_of` is how far the online-writer has applied the partition, not how long
ago this entity last changed. It is the `ingest_time` of the last event applied
on that partition. When the writer has reached the end of the partition and has
nothing further to apply, it sets `as_of` to its own current UTC time, at
millisecond precision. A caught-up writer refreshes that timestamp often enough
that a healthy idle partition stays inside the smallest `stale_after` (5 s).
The heartbeat is writer state, kept next to the per-partition offset watermark
(ADR-003). It is not a `FeatureSnapshot` and it is not a record on
`features.updates`.

The API finds a card's partition with `partition_for(card_token, n_partitions)`:
murmur2 of the UTF-8 key, the same partitioner the producer uses, and the
topic's partition count. Every card on that partition shares that `as_of`.

A merchant has no single partition. The log is keyed by `card_token`, and
merchant state is updated from every partition, so merchant `as_of` is the
oldest partition watermark. One lagging partition makes merchant features
stale. When every writer is caught up, heartbeats keep them fresh.

`age_ms` is the non-negative whole number of milliseconds between the API's
now and `as_of`.

A stream feature is `stale` when `age_ms > stale_after` for that feature, and
`fresh` when the entity has data and `age_ms` is within `stale_after`. A value
equal to the threshold is still fresh.

When the entity has no data, each of its features is `default`: the catalog
default, with `as_of` and `age_ms` unset. On-demand `geo_velocity_kmh` is not
part of the snapshot and is not covered by `stale_after`. Without coordinates
it is `default` (ADR-000).

`fallback` is defined in stage 5, for the path used when Redis is down.
`FeatureStatus.fallback` exists for that status.

## Alternatives considered

- **`as_of` equal to the entity's own last `ingest_time`** — an idle card would
  go stale while its writer is caught up, so a quiet card looks unhealthy.
- **Publishing heartbeats on `features.updates`** — freshness would depend on
  another message type, and the offline store would have to ignore those
  records.
- **Hashing `merchant_id` to pick one partition** — that partition is not where
  the merchant's events were written.
- **Calling the Kafka client's partitioner from `libs/schemas`** — the contract
  package would depend on a client. `partition_for` is the same murmur2
  function the producer uses.

## Consequences

- Serving freshness measures writer progress. A lagging partition makes every
  card on it, and every merchant, stale together.
- Training export still joins on snapshot `event_time` (ADR-004). `as_of` is a
  serving field and is not stored on the snapshot.
- The Redis key that holds the time watermark is chosen with the feature
  service, beside the offset watermark from ADR-003. This ADR fixes the
  meaning, not the key layout.
- `fallback` stays unspecified until stage 5.
