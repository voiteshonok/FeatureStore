-- SQL measurement stand (ADR-015). Not part of the product.
-- Loaded once, after 20-seed.sql, when the Postgres volume is created.
-- svc_baseline is created in 00-roles.sh and is not a member of fs_read / fs_write.

CREATE SCHEMA IF NOT EXISTS baseline;

COMMENT ON SCHEMA baseline IS
    'Measurement stand, not part of the product (ADR-015).';

CREATE TABLE IF NOT EXISTS baseline.transactions (
    event_id uuid PRIMARY KEY,
    card_token text NOT NULL,
    merchant_id text NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    status text NOT NULL CHECK (status IN ('approved', 'declined')),
    event_time timestamptz NOT NULL,
    ingest_time timestamptz NOT NULL,
    lat double precision,
    lon double precision
);

-- Card window and the latest geo point: one range scan per card.
CREATE INDEX IF NOT EXISTS transactions_card_event_time_idx
    ON baseline.transactions (card_token, event_time DESC);

-- Merchant minute-bucket window: one range scan per merchant.
CREATE INDEX IF NOT EXISTS transactions_merchant_event_time_idx
    ON baseline.transactions (merchant_id, event_time DESC);

REVOKE ALL ON SCHEMA baseline FROM PUBLIC;

GRANT USAGE ON SCHEMA baseline TO svc_baseline;
GRANT SELECT, INSERT ON baseline.transactions TO svc_baseline;
