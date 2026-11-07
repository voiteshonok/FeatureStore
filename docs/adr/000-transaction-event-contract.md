# ADR-000: TransactionEvent is the Kafka contract

## Status

Accepted

## Context

Ingestion accepts a full payment payload, including PAN. Downstream services
(features, detection, gateway) must share one event schema on `transactions.raw`.

## Decision

`TransactionEvent` is the only model published to Kafka and consumed downstream.
It has no `pan` field. The card is represented by `card_token`. The model is
frozen and rejects unknown fields (`extra="forbid"`).

HTTP input uses a separate `PaymentIn` model that includes `pan`. Ingestion maps
`PaymentIn` to `TransactionEvent` and drops PAN before produce.

## Consequences

Passing PAN past ingestion is a type error, not a runtime convention. Changing
this contract requires updating this ADR.
