# ADR-016: Two-level merchant buckets

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** ADR-002, ADR-005, K1, R4; amends merchant storage in ADR-002

## Context

ADR-002 stores one Redis hash per UTC minute per merchant and reads a 24 h window
as the sum of the last 1,440 minute hashes (`HMGET` × 1,440 per `/v1/features`
request). The 2026-11-17 direct-only load ladder
(`experiments/results/20261117-ladder/`) shows that pattern saturates
`features-api` (≈100% CPU) with ~10–15k `HMGET`/s even at 20 RPS requested —
p99 multi-second, gateway `503` under the same read cost — while Redis CPU stays
low. Gateway `GATEWAY_RATE_LIMIT=400/second` was ruled out (no `429`).

Minute-accurate window bounds must stay identical to ADR-005 / ClickHouse
parity: the merchant 24 h window remains the buckets
`(minute(T) − 1440, minute(T)]`.

## Decision

- **Keep minute hashes** (`mbkt:{merchant}:{minute}`) as the source of truth for
  partial hours at the window edges.
- **Add hourly roll-ups** (`mhbk:{merchant}:{hour}`, `hour = minute // 60`)
  updated atomically in the same Lua apply as the minute hash (count / sum /
  declined, same TTL policy).
- **Read path:** sum complete hours that lie entirely inside the 24 h minute
  window via hourly keys, and sum only the incomplete left/right edge minutes
  via minute keys (~23 hourly + ≤120 minute reads instead of 1,440).
- **Storage:** one Redis hash per merchant (`mfeat:{id}`) with fields
  `m:{minute}:c|d|s` and `h:{hour}:c|d|s` (dual-written in apply.lua). Legacy
  `mbkt:` / `mhbk:` per-bucket keys are no longer written.
- **Read execution:** `merchant_read.lua` takes that one key plus
  `[first, current]` and sums the ADR-016 plan inside Redis (one `EVAL` RTT);
  `/v1/features` also fetches watermarks, card, merchant and geo concurrently.
- Semantics of the window (which events count) are unchanged; only the Redis
  access pattern changes.

## Alternatives considered

- **1 s in-process merchant cache** — hides the symptom for hot keys; wrong as
  the diploma answer and breaks freshness under sparse merchants.
- **MGET / Redis modules / Redis Cluster** — does not reduce command count per
  request enough on a single-node stand; layout change is the durable fix.

## Consequences

- Warm-up / apply must dual-write; data written before this ADR has no hourly
  keys and would under-count until re-warmed.
- ClickHouse parity SQL still uses the same minute-bucket definition; it does
  not need hourly tables for correctness (offline can aggregate from events).
- Re-run a short direct ladder and a valid `exp-k1k2` (lag 0, errors &lt;1%,
  window-only K2) before claiming R4/K1.
