-- ADR-004. The entrypoint creates CLICKHOUSE_DB and then runs this file
-- without selecting it, so names are qualified.
CREATE DATABASE IF NOT EXISTS featurestore;

-- Raw transactions.raw history. ReplacingMergeTree collapses a redelivery of
-- the same event_id; exact reads use FINAL (ADR-004).
CREATE TABLE IF NOT EXISTS featurestore.events (
    schema_version UInt8,
    event_id UUID,
    payment_id String,
    merchant_id String,
    amount_minor Int64,
    currency LowCardinality(String),
    status Enum8('approved' = 1, 'declined' = 2),
    event_time DateTime64(3, 'UTC'),
    card_token String,
    lat Nullable(Float64),
    lon Nullable(Float64),
    ingest_time DateTime64(3, 'UTC'),
    kafka_partition UInt32,
    kafka_offset UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(event_time)
ORDER BY (card_token, event_time, event_id);

-- Values actually computed, for fallback and training export.
-- Latest snapshot per entity: argMax(..., event_time).
CREATE TABLE IF NOT EXISTS featurestore.feature_snapshots (
    entity_type Enum8('card' = 1, 'merchant' = 2),
    entity_id String,
    event_id UUID,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    features Map(LowCardinality(String), Float64),
    written_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree
ORDER BY (entity_type, entity_id, event_time, event_id);
