# ADR-017: Redis key lifetime and event-level dedup

- **Status:** Accepted
- **Date:** 2026-11-21
- **Related:** ADR-002, ADR-003, ADR-006, ADR-016; partially supersedes the TTL
  policy of ADR-016

## Context

ADR-016 dual-writes minute and hour fields into one `mfeat:{merchant}` hash and
kept a single key-level `EXPIRE` of 25 h set once on first create. An active
merchant then lost the whole hash after 25 h even though newer fields still
belonged in the 24 h window, while stale fields inside the hash were never
removed earlier. `card:` and `geo:` keys had no TTL at all, so a quiet card kept
its last day of members forever and the ADR-002 memory estimate drifted on long
runs.

Offset watermarks (ADR-003) stop Kafka redelivery of the same record. They do
not stop the same `event_id` at a new offset — the known gap in ADR-006 (Kafka
ack succeeded, PostgreSQL `COMMIT` of the idempotency row did not). The card
sorted set already ignores a repeated `event_id|amount` member; merchant
`HINCRBY` did not, so that duplicate doubled merchant features. ADR-006 requires
downstream to tolerate that duplicate.

## Decision

- **TTL from `event_time`, not wall-clock write time.** Absolute expiry moments
  are computed in Python and passed into `apply.lua`. A replay of an old log
  (rebuild) whose expiry is already in the past deletes the field or key
  immediately instead of leaving garbage.
- **Minute fields** `m:{min}:c|s|d`: `HPEXPIREAT mfeat:{id} <end of minute +
  24 h + 1 min> FIELDS 3 …`.
- **Hour fields** `h:{hour}:c|s|d`: the same from the end of the hour.
- **No key-level `EXPIRE` on `mfeat`.** Redis removes an empty hash when the last
  field expires.
- **`card:` and `geo:`:** `PEXPIREAT <event_time + 24 h + 1 min>`. Use the `GT`
  flag when the key already has a TTL so a late event never shortens it. A key
  with no TTL is unaffected by `GT`, so the first set uses plain `PEXPIREAT`.
  Geo updates use `SET … KEEPTTL` so rewriting the value does not drop the
  previous expiry before `GT` runs.
- **Event-level dedup:** the `ZADD` return value is the number of new members.
  When it is `0`, the event was already applied: skip merchant increments and
  geo writes, advance the partition watermark, return `dup_event`. The online
  writer must not publish snapshots for `dup_event` (detection would see a
  repeat).
- **Method boundary:** a duplicate whose member was already trimmed out of the
  card sorted set (older than 24 h) is not recognised. Features are unaffected:
  its minute is outside the window and the field expires immediately.

## Alternatives considered

- **Separate `event_id` set with 24 h TTL** — rejected in ADR-003 (~17 M keys,
  >1 GB); reusing the card member is enough for the ADR-006 gap while the member
  remains in the window.
- **Refresh a single `mfeat` key TTL on every write** — still leaves stale
  minute/hour fields until the whole key dies; field-level expiry matches the
  window.

## Consequences

- `apply.lua` ARGV carries absolute `minute_expire_at_ms`, `hour_expire_at_ms`,
  and `key_expire_at_ms` instead of a relative merchant TTL.
- Outcome `dup_event` is counted separately from offset `dup` in writer counters
  and `features_events_total`.
- Stand keys written under the old policy are not migrated; `make clean`
  recreates the stand.
- `merchant_read.lua` is unchanged.
