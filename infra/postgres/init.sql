CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE TABLE IF NOT EXISTS ingestion.idempotency_keys (
    event_id uuid PRIMARY KEY,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idempotency_keys_received_at_idx
    ON ingestion.idempotency_keys (received_at);

-- 25 h = 24 h acceptance window (ADR-000) plus a margin. Called on a timer by ingestion (FS-17).
CREATE OR REPLACE FUNCTION ingestion.delete_expired_idempotency_keys()
RETURNS void
LANGUAGE sql
AS $$
    DELETE FROM ingestion.idempotency_keys
    WHERE received_at < now() - interval '25 hours';
$$;

-- Feature registry. The catalog in code is the source of truth (ADR-012).
-- svc_features is the API role. The password matches infra/.env.example.
-- This script runs once, when the Postgres volume is created.
CREATE ROLE svc_features LOGIN PASSWORD 'featurestore';

CREATE SCHEMA IF NOT EXISTS features;

CREATE TABLE IF NOT EXISTS features.feature_definitions (
    name text PRIMARY KEY,
    entity text NOT NULL,
    kind text NOT NULL,
    agg text NOT NULL,
    default_value double precision
);

CREATE TABLE IF NOT EXISTS features.feature_versions (
    name text NOT NULL REFERENCES features.feature_definitions (name),
    version integer NOT NULL,
    window_ms bigint,
    stale_after_ms bigint,
    PRIMARY KEY (name, version)
);

GRANT USAGE ON SCHEMA features TO svc_features;
GRANT SELECT, INSERT, UPDATE
    ON features.feature_definitions, features.feature_versions
    TO svc_features;
