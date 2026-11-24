# Best practices

Short rules for clients of the gateway. The worked examples are in
[Getting started](getting-started.md) and the
[alert triage tutorial](alert-triage-tutorial.md).

## Retry only safe calls

Retry `GET` (alerts, rules, catalog, `GET /v1/auth/me`) with backoff when the
status is `429`, `503`, or `504`. Honour `Retry-After` on `429`.

Do not retry `POST /v1/features` or `PATCH /v1/alerts/{id}` automatically.
A feature read is not idempotent from the client's point of view (the vector
can change), and a status change must not be applied twice. Ingestion is the
exception: repeating the same `event_id` is safe and returns `duplicate: true`.

## Branch on `type`

Every error body is Problem Details. `type` is the stable code. `title` is for
humans and can change. The list and the recovery step for each type are in
[the error taxonomy](../api-errors.md).

## Log `request_id`

The gateway returns `X-Request-ID` on every response and copies it into
`request_id` in the error body. Log that value with the call. It is the key
for the gateway log line and the upstream log line.

## Read `status` on each feature

| status | Meaning |
| --- | --- |
| `fresh` | Inside the freshness budget. Use the value. |
| `stale` | Older than the budget. Use it only if a late value is acceptable, and log it. |
| `fallback` | The online store was unavailable. Degrade the decision. |
| `default` | This entity has no observations. The number is the catalog default, not a measured zero. Treat it as unknown. |

`default` is not "zero risk". A new card with `card_cnt_1m.status = default`
has not been seen; do not score it as a quiet card.

## Never send a PAN to the gateway

`POST /v1/features` takes a `card_token`, not a card number. PANs are accepted
only by ingestion (`POST /v1/events`), which tokenizes them before they are
published. A test PAN such as `4111111111111111` belongs in ingestion examples,
not in gateway calls, logs, or tickets.
