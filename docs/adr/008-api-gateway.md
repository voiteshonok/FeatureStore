# ADR-008: API gateway

- **Status:** Proposed (decide before Stage 7)
- **Date:** 2026-11-07
- **Related:** NFR-9, US-6.5; complexity criterion "Microservices"

## Context

External clients (scoring service, dashboard) need one entry point with authentication
(JWT for dashboard users, API keys for the scoring service), routing to internal
services, rate limiting (100 requests per second per client) and TLS. The gateway adds
a network hop to the feature read path, so its latency counts towards K1.

## Alternatives considered

- **Custom FastAPI gateway** — full control, easy to explain in the thesis, more code
  to write and test.
- **Traefik with forward-auth** — mature routing, TLS and rate limiting out of the box;
  authentication stays in a small auth service.

## Decision

Not taken yet. Criteria: added p99 latency on the feature path, implementation effort,
how clearly the choice can be justified for the Microservices criterion.

## Consequences

To be filled in when the decision is taken.
