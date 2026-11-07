# ADR-009: PAN stays in ingestion, card_token is HMAC

## Status

Accepted

## Context

NFR-8: PAN must never appear in Kafka, stores, or logs. Downstream still needs
a stable per-card identifier for features.

## Decision

Ingestion hashes PAN with HMAC-SHA256 using `secrets/hmac_key` (32 random bytes,
hex) and sets `TransactionEvent.card_token`. PAN is discarded before the event
is serialised. The HMAC key is not committed; `make secret` creates it locally.

## Consequences

The same PAN always maps to the same token for a given key. Rotating the key
changes tokens and breaks feature history unless a migration is designed.
