# ADR-000: Transaction event schema

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** US-1.1, US-1.2, US-1.3, US-2.3, US-2.4, NFR-8, K2; ADR-009

## Context

Every service reads or writes transaction events. A schema change after feature
computation exists would require rewriting ingestion, both writers, the ClickHouse
DDL and the tests, so the contract is fixed first. The BA mentions payment retries
and late status updates (L4); it has to be clear what counts as a duplicate and what
counts as a new event. The PAN must never leave the ingestion service.

## Decision

- **One event = one authorization attempt.** Events are immutable; the status is
  known at once. Status updates, refunds and chargebacks are out of scope.
- `event_id` (UUID) identifies a delivery: the same `event_id` twice is a duplicate.
  `payment_id` links retries: a retry has the same `payment_id` and a new `event_id`
  and is counted as a separate event.
- Two models: `PaymentIn` (ingestion HTTP input, has `pan`) and `TransactionEvent`
  (Kafka and downstream, no `pan` field at all).
- `TransactionEvent` fields: `schema_version`, `event_id`, `payment_id`, `card_token`,
  `merchant_id`, `amount_minor`, `currency`, `status`, `lat`, `lon`, `event_time`,
  `ingest_time`.
- Amounts are integers in euro cents; `currency` is always `EUR`.
- Timestamps are UTC with millisecond precision. `event_time` is set by the source,
  `ingest_time` by ingestion (needed to measure K2).
- Ingestion rejects with 422: `event_time` more than 5 s ahead of `ingest_time` or
  older than 24 h; currency other than EUR; PAN failing the Luhn check.
- `lat` / `lon` are optional; without them geo-velocity returns status `default`.
- Kafka payload: JSON serialized from the Pydantic model, with `schema_version`.

## Alternatives considered

- **Event per state change** (authorized, status updated, refunded) — features would
  have to correct already computed window values, and parity would become much harder,
  with no gain for the research questions.
- **Decimal amounts / multiple currencies** — float issues in JSON and ClickHouse and
  currency conversion for window sums; not needed for the topic.
- **Avro or Protobuf with a Schema Registry** — an extra component on a single server;
  serialization is not the bottleneck for NFR-1.

## Consequences

- Duplicates and retries have unambiguous meaning; idempotency keys on `event_id`.
- PAN isolation is enforced by types, not by discipline.
- Late events are accepted within 24 h, which equals the longest window.
- Limitation: no status updates. Avro with a registry is a future extension.
