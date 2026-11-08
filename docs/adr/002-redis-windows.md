# ADR-002: Window storage in Redis

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** US-1.3, US-2.1, US-2.3, US-2.4, K1, K4; ADR-003, ADR-005

## Context

Card features need sliding windows of 1 min, 1 h and 24 h (US-2.1); merchant features
need 24 h counts and decline rate (US-2.3). At 200 TPS a day holds about 17.3 million
events. The whole system runs on one VM with 16 GB, shared with Kafka, ClickHouse and
PostgreSQL. Windows must be exact for cards (acceptance criterion "after N events the
value is N"), independent of arrival order (US-2.4) and safe against duplicates.

Simulator profile used for the estimate: about 3 million cards with about 5 payments a
day each, about 5,000 merchants. Cards therefore have few events each; merchants have
thousands.

## Decision

- **Cards — every event is stored.** One sorted set per card covering 24 h:
  `score = event_time` (ms), `member = "event_id|amount"`. The 1 min, 1 h and 24 h
  values are range queries on the same key. Old members are removed with
  `ZREMRANGEBYSCORE`. Windows are exact and identical members deduplicate.
- **Merchants — minute buckets.** One hash per merchant with fields per minute:
  count, sum, declined. A 24 h window is the sum of the last 1,440 buckets, so the
  window boundary has one-minute granularity. ClickHouse uses the same bucket
  definition for parity.

## Alternatives considered

- **Every event for cards and merchants** — exact, but merchants hold thousands of
  events each; estimated about 3 GB in total.
- **Buckets for everything** — smallest footprint (about 0.6 GB), but the 1 min card
  window becomes approximate and duplicates cannot be recognised in the data.

## Consequences

- Estimated memory about 1.5 GB; verified with `MEMORY USAGE` on warmed-up data
  in Stage 3.
- Experiments need a warm-up (replay of a day of history) before measuring K1 and K3.
- Merchant buckets cannot detect duplicates by content, so deduplication relies on
  ADR-003.
