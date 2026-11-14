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
