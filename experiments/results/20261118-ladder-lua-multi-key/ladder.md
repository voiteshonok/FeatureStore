# Read-path load ladder (direct :8001)

No backfill, simulator off. Isolates merchant HMGET read path.

| RPS | p50 | p99 | errors | Redis ops/s | HMGET/s | Redis CPU | API CPU | lag max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 | 8.23 ms | 17.73 ms | 0.000% | 1673.8 | 1593.1 | 4.1% | 41.1% | 0.0 |
| 50 | 7.28 ms | 12.91 ms | 0.000% | 4115.8 | 4002.4 | 5.7% | 38.1% | 0.0 |
| 100 | 7.60 ms | 766.18 ms | 4.770% | 7520.0 | 7443.7 | 14.0% | 66.3% | 0.0 |
| 200 | 319.30 ms | 1146.17 ms | 23.617% | 8731.8 | 12786.3 | 16.9% | 107.0% | 0.0 |

## Error classes

| RPS | 2xx_check_fail | 4xx | 429 | 503 | timeout | other |
|---:|---:|---:|---:|---:|---:|---:|
| 20 | 0 | 0 | 0 | 0 | 0 | 0 |
| 50 | 0 | 0 | 0 | 0 | 0 | 0 |
| 100 | 0 | 0 | 0 | 143 | 0 | 0 |
| 200 | 0 | 0 | 0 | 1417 | 0 | 0 |

## Knee

- First bad step: **100 RPS** (p99=766.2ms ≫ 20ms, error_rate=4.77%).

## Host / Docker

- host CPUs: 12, mem: 31962704 kB
- docker CPUs: 12, mem: 32729808896
- hypothesis: merchant read path: ADR-016 two-level buckets (~23 hourly + ≤120 minute HMGET/req). Pre-ADR-016 baseline was 1440 HMGET/req.
