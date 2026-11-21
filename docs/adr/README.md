# Architecture Decision Records

One file per decision. Accepted ADRs are not rewritten; a changed decision gets a
new ADR that supersedes the old one.

| ADR | Title | Status | Date |
| --- | --- | --- | --- |
| [000](000-event-schema.md) | Transaction event schema | Accepted | 2026-11-07 |
| [001](001-consumer-groups.md) | Independent online and offline writers | Accepted | 2026-11-07 |
| [002](002-redis-windows.md) | Window storage in Redis | Accepted | 2026-11-07 |
| [003](003-delivery-semantics.md) | Delivery semantics and deduplication | Accepted | 2026-11-07 |
| [004](004-offline-storage.md) | Offline storage layout in ClickHouse | Accepted | 2026-11-07 |
| [005](005-parity-semantics.md) | Semantics of online/offline parity | Accepted | 2026-11-07 |
| [006](006-ingestion-on-kafka-outage.md) | Ingestion behaviour when Kafka is unavailable | Accepted | 2026-11-14 |
| [007](007-feature-service-layout.md) | Feature service layout | Accepted | 2026-11-07 |
| [008](008-api-gateway.md) | API gateway | Accepted | 2026-11-07 |
| [009](009-tokenization-secret.md) | Tokenization secret | Accepted | 2026-11-07 |
| [012](012-feature-catalog.md) | Feature catalog as code | Accepted | 2026-11-08 |
| [013](013-freshness-semantics.md) | Freshness semantics | Accepted | 2026-11-15 |
| [014](014-observability-metrics.md) | Observability: metrics | Accepted | 2026-11-16 |
| [015](015-sql-baseline.md) | SQL baseline | Accepted | 2026-11-16 |
| [016](016-merchant-two-level-buckets.md) | Two-level merchant buckets | Accepted | 2026-11-18 |
| [017](017-redis-key-lifetime-and-event-dedup.md) | Redis key lifetime and event-level dedup | Accepted | 2026-11-21 |
| [018](018-offline-writer-batching.md) | Offline writer batching and commit | Accepted | 2026-11-21 |
| [019](019-orm-detection-api.md) | ORM in the detection API | Accepted | 2026-11-21 |
