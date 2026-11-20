# Experiments

Measurement packs for diploma criteria K1/K2 and Redis memory (R4).

**Do not use** [`results/20261117-k1-k2/`](results/20261117-k1-k2/) for diploma
numbers: that pack is marked `validity: INVALID` (pipeline failure under read
load). Pre-fix HMGET ladder:
[`results/20261117-ladder-pre-adr016/`](results/20261117-ladder-pre-adr016/).
Post–ADR-016 smoke ladder: [`results/20261118-ladder/`](results/20261118-ladder/).
Use a later valid `exp-k1k2` pack for diploma K1/K2.

## Read-path ladder (`make exp-ladder`)

Direct `:8001` only, **no backfill / simulator**, steps **20 / 50 / 100 / 200 RPS ×
2 min**. Samples Redis `instantaneous_ops_per_sec`, `cmdstat_hmget`, docker CPU,
and consumer lag every ~5 s. Writes `experiments/results/<UTC-date>-ladder/` with
`ladder.md` (marks the knee). Use this before blaming gateway 429 or changing
ADR-002 merchant buckets.

```bash
make exp-ladder
# or reuse a running stand:
SKIP_SETUP=1 make exp-ladder
```

## K1 / K2 + Redis (`make exp-k1k2`)

One command resets the stand (clean volumes), warms feature windows, samples
Redis, runs the simulator at 200 TPS, and measures three k6 targets
(direct `:8001`, gateway `:8080`, baseline `:8003`). Artifacts land in
`experiments/results/<UTC-date>-k1-k2/`:

- `config.json` — SHA, seeds/run-ids, warm-up, k6 knobs, `GATEWAY_RATE_LIMIT`,
  container memory limits, images, host CPU/RAM, load-generator placement
- `k6-direct.json`, `k6-gateway.json`, `k6-baseline.json`
- `prometheus.json` — K2 p95 windows and status shares
- `redis-memory.json` — `INFO memory`, `DBSIZE`, `MEMORY USAGE` samples
- `report.md` — comparison table and notes

### Prerequisites

- Docker Compose, `uv`, Python 3.12
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) on `PATH`
- See also [load/README.md](load/README.md) for the shared k6 scenario

### Run

```bash
make exp-k1k2
# faster pipeline check (same 30s warm-up, k6 1m per target):
make exp-k1k2-short
```

Environment overrides (optional): `BACKFILL`, `TPS`, `SEND_RATE`, `SEED`,
`RATE`, `DURATION`, `PAUSE_S`, `GATEWAY_RATE_LIMIT`, `MODE=short`.

### How long

| Mode | Warm-up | k6 | Rough wall clock |
|------|---------|----|------------------|
| default (`exp-k1k2`) | `BACKFILL=30s` (~6k events, quick catch-up) | 3×10m @ 200 RPS | **~40 min** |
| short (`exp-k1k2-short`) | same 30s | 3×1m @ 50 RPS | ~10–15 min |
| deeper warm-up | `BACKFILL=3m` or `23h` | 3×10m | minutes to ~1+ day for catch-up |

Default warm-up is **not** a full-day ADR-002 replay. Redis will be far below
the ~1.5 GB estimate; 24h windows are only partially filled. For a day-scale
check later: `BACKFILL=23h make exp-k1k2`.

### Traps

- Gateway defaults to 100 req/s (ADR-008). The experiment sets
  `GATEWAY_RATE_LIMIT=400/second` so a 200 RPS gateway run does not flood with
  429.
- Read load is measured **under write load**: the simulator keeps 200 TPS for
  the whole k6 window. Without that, K1 is understated and K2 is not observed.
- k6 runs with `--no-thresholds` so a missed K1 still produces a full results
  pack; misses are called out in `report.md` under **Causes**.
- The load generator (k6) runs on the **same host** as Compose in this make
  target; that is recorded in `config.json` / `report.md`.

### SQL baseline stand

Profile `baseline` (ADR-015) is started by the orchestrator. It is a measurement
stand, not a product service.
