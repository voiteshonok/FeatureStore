# ADR-007: Feature service layout

- **Status:** Accepted
- **Date:** 2026-11-07
- **Related:** NFR-1, NFR-11; ADR-001

## Context

The feature service both consumes events (write path) and answers feature requests
(read path). The two paths scale differently: NFR-11 requires throughput to grow when
consumers are added, while the API must keep low latency regardless of consumer load.

## Decision

One Python package `features` with shared feature logic and storage code, built into
two container images:

- **`worker`** — runs the online-writer and offline-writer consumers.
- **`api`** — FastAPI service for `POST /v1/features` and health endpoints.

## Alternatives considered

- **One process doing both** — consumer load affects API latency; cannot scale
  consumers separately.
- **Two separate packages** — duplicated storage and feature code.

## Consequences

- Workers scale horizontally up to the number of partitions (4) for NFR-11.
- The API can be restarted or scaled without touching consumer offsets.
- Both images must be built and tested in CI.
