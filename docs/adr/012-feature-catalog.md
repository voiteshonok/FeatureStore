# ADR-012: Feature catalog as code

## Status

Accepted

## Context

The feature worker, feature API, parity check and dashboard must agree on the
same list of features. Duplicating names and windows across services makes
additions and changes error-prone (G3, P3).

## Decision

The catalog lives in `libs/schemas` as a frozen `FeatureSpec` tuple (`CATALOG`).
Lookups are `by_name` and `for_entity`. Stream features declare `window` and
`stale_after`; on-demand features declare neither. No window exceeds 24 h, the
event acceptance horizon from ADR-000.

Adding or changing a feature is an edit to this catalog (and this ADR if the
contract changes), not to each consumer.

## Consequences

All producers and consumers of feature names import `CATALOG`. A missing or
renamed feature fails at lookup (`KeyError`) instead of silently diverging.
