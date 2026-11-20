# Gateway rate-limit check (2026-11-18)

- Live `GATEWAY_RATE_LIMIT=400/second` on `infra-gateway-1` (no recreate needed).
- Burst 200 parallel POSTs `/v1/features` with valid `X-API-Key`: **200 × 503** (upstream timeout / unavailable), **0 × 429**.
- Missing API key: **401**. Bad key: **403**.
- Direct `:8001` under a 50-way parallel burst: **50 × 200** (`direct-status.txt`).

Conclusion: at 400/s limit, **429 is ruled out**. Gateway failures under load are **503**, consistent with features-api latency / Redis saturation, not the rate limiter.
