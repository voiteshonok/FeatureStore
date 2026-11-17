# K1 / K2 report

| Показатель | Цель | Baseline | Online напрямую | Online через gateway |
|---|---|---|---|---|
| K1: p99 /v1/features, 200 RPS | ≤ 20 мс | 2635.54 мс | 13128.93 мс | 13714.79 мс |
| K1: p50 | ≤ 5 мс | 2063.71 мс | 1910.63 мс | 10566.85 мс |
| K2: p95 событие → хранилище | ≤ 2 с | 0.758 с | 30.000 с | 30.000 с |
| Доля ошибок | < 0.1% | 0.000% | 98.803% | 100.000% |

## Notes

- Redis `used_memory` = **0.005 GB** (`4.64M`), DBSIZE = 16296. ADR-002 day-scale estimate is ~1.5 GB; this run used warm-up depth **30s**, so memory is not comparable to the full-day figure and 24h windows are only partially filled.
- Warm-up achieved ingest rate: **n/a** (TPS=200.0, send-rate=2000.0).
- Feature value statuses during runs: fresh=91.67%, stale=n/a, default=8.33%.
- Load generator placement: k6 ran on the same host as the Docker Compose stand (colocated).
- Measurement limits: Warm-up depth 30s is not a full-day ADR-002 replay; 24h windows are only partially filled; Redis memory is not comparable to the ~1.5 GB day estimate..

## Causes

Threshold misses:
- direct K1 p99 = 13128.93 ms > 20 ms
- direct K1 p50 = 1910.63 ms > 5 ms
- direct error rate = 98.8027% ≥ 0.1%
- gateway K1 p99 = 13714.79 ms > 20 ms
- gateway K1 p50 = 10566.85 ms > 5 ms
- gateway error rate = 100.0000% ≥ 0.1%
- direct K2 p95 = 30.000 s > 2 s
- gateway K2 p95 = 30.000 s > 2 s

Likely reasons (this short pack):
- Load generator was **colocated** with Compose on one host; k6 at 200 RPS
  plus the live simulator contended for CPU/Redis with features-api/worker.
- Online writer fell behind (K2 histogram mass in the `+Inf` / 30 s bucket);
  during some k6 windows the worker applied ~0 events, so in-window K2 was
  empty and the report fell back to the cumulative histogram.
- Gateway/direct error spikes match upstream timeouts under that saturation
  (checks failed even when some HTTP bodies returned).
- Warm-up was only **30s**, so this is a pipeline check, not a day-scale R4.

Stage 4 must not start until these are understood and accepted. Re-run with
k6 on a separate machine and/or lower RATE, after confirming
`sum(features_consumer_lag)==0` and steady write load.
