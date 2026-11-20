# Read-path load ladder (direct :8001) — after Lua + mfeat + workers

No backfill, simulator off. Isolates merchant read path.

## Pack: mfeat hash + merchant_read.lua + FEATURES_API_WORKERS=4

| RPS | p50 | p99 | errors | Redis ops/s | API CPU |
|---:|---:|---:|---:|---:|---:|
| 20 | 8.11 ms | **12.71 ms** | 0.000% | 3109.8 | 44.2% |
| 50 | 7.54 ms | **12.20 ms** | 0.000% | 7511.0 | 33.4% |
| 100 | 7.50 ms | 22.05 ms | 0.000% | 14080.5 | 82.7% |
| 200 | 8.85 ms | 675.44 ms | 0.183% | 28168.8 | 114.1% |

Knee: **200 RPS** (p99 tail). At 100 RPS p99 is just over the 20 ms K1 bound.

`cmdstat_hmget` is 0 — reads use `HGET` inside Lua on `mfeat:{id}`.

## Progression vs earlier packs

| Stage | 20 RPS p99 | 50 RPS p99 | 100 RPS | 200 RPS |
|---|---:|---:|---|---|
| Pre-ADR-016 (1440 HMGET) | ~10.7 s | ~12.9 s | 95% 503 | 99% 503 |
| ADR-016 multi-key pipeline | 24 ms | 29 ms | 12% 503 | — |
| + Lua EVAL (multi-key) | 18 ms | 13 ms | 4.7% 503 | bad |
| + mfeat + 4 workers | **13 ms** | **12 ms** | 0% err, p99 22 ms | p50 9 ms, p99 675 ms |

## Notes

- K1 goals: p50 ≤ 5 ms, p99 ≤ 20 ms @ **200 RPS**. p50 floor is still ~7–9 ms; 200 RPS p99 not met.
- 8 workers @ 200 RPS did **not** help (p99 ~942 ms) — more Redis contention.
- Next for 200 RPS: true O(1) materialized 24h counter on write, and/or cut per-request work for p50 (watermarks cache, leaner JSON).
