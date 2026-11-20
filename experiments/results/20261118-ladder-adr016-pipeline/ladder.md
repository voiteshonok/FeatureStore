# Read-path load ladder (direct :8001)

No backfill, simulator off. Isolates merchant HMGET read path.

| RPS | p50 | p99 | errors | Redis ops/s | HMGET/s | Redis CPU | API CPU | lag max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 | 14.09 ms | 24.31 ms | 0.000% | 1675.0 | 1605.3 | 2.3% | 25.0% | 0.0 |
| 50 | 14.33 ms | 28.96 ms | 0.000% | 4155.8 | 4008.0 | 7.0% | 93.7% | 0.0 |
| 100 | 751.69 ms | 2066.40 ms | 12.164% | 6873.0 | 7291.5 | 9.8% | 122.4% | 0.0 |

## Error classes

| RPS | 2xx_check_fail | 4xx | 429 | 503 | timeout | other |
|---:|---:|---:|---:|---:|---:|---:|
| 20 | 0 | 0 | 0 | 0 | 0 | 0 |
| 50 | 0 | 0 | 0 | 0 | 0 | 0 |
| 100 | 0 | 0 | 0 | 358 | 0 | 0 |

## Knee

- First bad step: **100 RPS** (p99=2066.4ms ≫ 20ms, error_rate=12.16%).

## vs pre-ADR-016 (`20261117-ladder-pre-adr016`)

| RPS | p99 before | p99 after (ADR-016) |
|---:|---:|---:|
| 20 | 10679 ms | **24 ms** |
| 50 | 12865 ms | **29 ms** |
| 100 | 12976 ms (95% err) | 2066 ms (12% err) |

Two-level buckets move the knee from **20 → 100 RPS**. Full 2 min × 200 ladder + valid `exp-k1k2` still needed before diploma K1 claims.

## Host / Docker

- host CPUs: 12, mem: 31962704 kB
- docker CPUs: 12, mem: 32729808896
- note: short verify run (`DURATION=30s`, rates 20/50/100) after ADR-016 deploy
