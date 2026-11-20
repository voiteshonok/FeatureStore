# Read-path load ladder (direct :8001)

No backfill, simulator off. Isolates merchant HMGET read path.

| RPS | p50 | p99 | errors | Redis ops/s | HMGET/s | Redis CPU | API CPU | lag max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 | 8892.99 ms | 10678.88 ms | 0.000% | 12800.5 | 15106.1 | 5.6% | 100.8% | 0.0 |
| 50 | 8423.01 ms | 12864.52 ms | 0.000% | 11373.0 | 15671.7 | 5.3% | 105.3% | 0.0 |
| 100 | 3063.35 ms | 12975.94 ms | 95.463% | 15982.9 | 14352.1 | 5.4% | 111.3% | 0.0 |
| 200 | 1934.70 ms | 10199.46 ms | 99.122% | 10741.5 | 10981.2 | 6.5% | 107.0% | 0.0 |

## Error classes

| RPS | 2xx_check_fail | 4xx | 429 | 503 | timeout | other |
|---:|---:|---:|---:|---:|---:|---:|
| 20 | 0 | 0 | 0 | 0 | 0 | 0 |
| 50 | 0 | 0 | 0 | 0 | 0 | 0 |
| 100 | 0 | 0 | 0 | 5050 | 0 | 0 |
| 200 | 0 | 0 | 0 | 16261 | 0 | 0 |

## Knee

- First bad step: **20 RPS** (p99=10678.9ms ≫ 20ms).

## Confirmation

- **HMGET read path confirmed.** `cmdstat_hmget` ≈ 11–15k/s while requested RPS is
  only 20–50; API container CPU ≈ 100%; Redis CPU stays ~5–6% (Python pipeline of
  1 440 `HMGET`s saturates `features-api` before Redis’s single thread).
- At 100/200 RPS, errors are **503** (not 429). Lag stays 0 (simulator off).
- Follow-up: ADR-016 two-level merchant buckets (not a 1 s cache).

## Host / Docker

- host CPUs: 12, mem: 31962704 kB
- docker CPUs: 12, mem: 32729808896
- hypothesis: merchant_stream_features issues 1440 HMGET per request; expect cmdstat_hmget ≈ 1440 × successful requests and Redis CPU knee
