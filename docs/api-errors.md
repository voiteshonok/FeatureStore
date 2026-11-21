# API error taxonomy

All services return the same Problem Details body (T7.2):

```json
{
  "type": "invalid_transition",
  "title": "invalid transition",
  "status": 409,
  "request_id": "6f1c…",
  "errors": []
}
```

`errors` is filled only for `validation_error` (422): `{ "field", "message" }`.
`type` is the stable code the client should switch on; `title` is human-readable
and may change. Correlate logs with `request_id` / `X-Request-ID`.

Gateway-owned errors (auth, rate limit, catalog, transport) use this shape.
Upstream HTTP bodies are proxied unchanged.

| type | status | When | Client action |
| --- | --- | --- | --- |
| `validation_error` | 422 | Body/query/header failed schema or business validation | Fix the named field(s) in `errors` |
| `unauthorized` | 401 | Missing credentials, invalid session, or bad login | Send API key or log in |
| `forbidden` | 403 | Unknown key, insufficient role, or missing CSRF header | Use a permitted identity / send `X-Requested-With: fetch` |
| `not_found` | 404 | Unknown alert id (and similar) | Stop retrying that id |
| `invalid_transition` | 409 | Alert status change not allowed | Reload alert; choose a legal status |
| `conflict` | 409 | Generic conflict when no narrower type applies | Retry after refresh |
| `rate_limited` | 429 | Gateway per-client limit | Honour `Retry-After` |
| `upstream_unavailable` | 503 | Dependency down (Kafka, Redis, catalog, connect error) | Retry with backoff |
| `upstream_timeout` | 504 | Upstream read/connect timeout at the gateway | Retry with backoff |
| `internal_error` | 500 | Unexpected exception | Retry once; report `request_id` |
| `http_error` | * | Unmapped `HTTPException` status | Treat by `status` |
| `bad_request` | 400 | Malformed request outside validation schema | Fix the request |

PAN digits in unexpected exceptions are masked in logs and never appear in the
500 body (`title` is always `Internal server error`).
