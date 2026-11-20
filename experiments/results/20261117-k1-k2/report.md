# K1 / K2 report — INVALID

**validity: INVALID**

This pack is a **pipeline failure artifact**, not a diploma measurement.
Do **not** compare any number below to K1/K2 goals or quote it in the thesis.

Root issues observed: error rates ≫ 1%, K2 histogram mass in `+Inf` (printed
as a false finite ceiling in an earlier draft), cumulative K2 substitution,
and a 30s warm-up. Primary design hypothesis for the next diagnosis step:
merchant reads issue **1 440 Redis HMGET** per `/v1/features` request
(`merchant_stream_features`), saturating Redis under ~200 RPS reads.

| Показатель | Цель | Baseline | Online напрямую | Online через gateway |
|---|---|---|---|---|
| K1: p99 /v1/features, 200 RPS | ≤ 20 мс | — (INVALID) | — (INVALID) | — (INVALID) |
| K1: p50 | ≤ 5 мс | — (INVALID) | — (INVALID) | — (INVALID) |
| K2: p95 событие → хранилище | ≤ 2 с | — (INVALID) | — (INVALID) | — (INVALID) |
| Доля ошибок | < 0.1% | — (INVALID) | — (INVALID) | — (INVALID) |

## Notes

- Raw k6 / prometheus / redis JSON files are kept only for debugging the
  pipeline and the R4 load ladder that follows.
- Stage 4 must not start from these numbers.
