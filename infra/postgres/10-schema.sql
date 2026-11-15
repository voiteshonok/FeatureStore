-- FeatureStore transactional schema (OLTP, PostgreSQL 17).
-- Supersedes init.sql. Structure is created only from these scripts (00-roles.sh →
-- 10-schema.sql → 20-seed.sql), never by hand; the directory is mounted into
-- /docker-entrypoint-initdb.d and runs once when the Postgres volume is created.
-- Roles live in 00-roles.sh (passwords from the environment — nothing in Git).

CREATE EXTENSION IF NOT EXISTS pgcrypto;  -- crypt()/gen_salt() for operator passwords

CREATE SCHEMA IF NOT EXISTS ingestion;  -- ingestion service: idempotency window
CREATE SCHEMA IF NOT EXISTS ref;        -- reference data
CREATE SCHEMA IF NOT EXISTS features;   -- feature registry (catalog in code is the source of truth, ADR-012)
CREATE SCHEMA IF NOT EXISTS detection;  -- alert rules and alerts
CREATE SCHEMA IF NOT EXISTS ops;        -- operators, audit

-- ---------------------------------------------------------------------------
-- ingestion
-- ---------------------------------------------------------------------------
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

-- ---------------------------------------------------------------------------
-- ref: merchants seen by the pipeline. The simulator draws ids m0…mN, so this is
-- a registry upserted by detection on first sight; descriptive attributes are
-- nullable and filled for known merchants by the seed (demo of a reference fill).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref.merchants (
    merchant_id   text PRIMARY KEY,
    display_name  text,
    category      text,
    city          text,
    risk_level    text NOT NULL DEFAULT 'unclassified'
                  CHECK (risk_level IN ('unclassified', 'low', 'medium', 'high')),
    first_seen_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- features: registry of the catalog in code (ADR-012). The features api upserts
-- it at startup; name is the stable key the services and rules join on.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS features.feature_definitions (
    name text PRIMARY KEY,
    entity text NOT NULL CHECK (entity IN ('card', 'merchant')),
    kind text NOT NULL CHECK (kind IN ('stream', 'on_demand')),
    agg text NOT NULL CHECK (agg IN ('count', 'sum', 'decline_rate', 'geo_velocity')),
    default_value double precision
);

CREATE TABLE IF NOT EXISTS features.feature_versions (
    name text NOT NULL REFERENCES features.feature_definitions (name),
    version integer NOT NULL CHECK (version >= 1),
    window_ms bigint CHECK (window_ms > 0),            -- NULL for on_demand
    stale_after_ms bigint CHECK (stale_after_ms > 0),  -- NULL for on_demand
    PRIMARY KEY (name, version)
);

-- ---------------------------------------------------------------------------
-- ops: dashboard operators and audit trail
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.operators (
    operator_id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email text NOT NULL UNIQUE
        CHECK (email = lower(email) AND position('@' IN email) > 1),
    -- bcrypt via pgcrypto crypt(); the format CHECK makes plain-text inserts impossible
    password_hash text NOT NULL CHECK (password_hash LIKE '$2%'),
    role text NOT NULL DEFAULT 'viewer' CHECK (role IN ('viewer', 'analyst', 'admin')),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Data masking for non-admin consumers of operator data.
CREATE OR REPLACE VIEW ops.operators_masked AS
SELECT operator_id,
       regexp_replace(email, '(^.).*(@.*$)', '\1***\2') AS email,
       role,
       created_at
FROM ops.operators;

CREATE TABLE IF NOT EXISTS ops.audit_log (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    actor text,
    action text NOT NULL,
    alert_id uuid NOT NULL,
    detail jsonb NOT NULL
);

-- ---------------------------------------------------------------------------
-- detection: rules (clause-level comparisons, M:N with features) and alerts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS detection.alert_rules (
    rule_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL UNIQUE,
    entity text NOT NULL CHECK (entity IN ('card', 'merchant')),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS detection.rule_features (
    rule_id uuid NOT NULL REFERENCES detection.alert_rules (rule_id),
    feature_name text NOT NULL REFERENCES features.feature_definitions (name),
    comparison text NOT NULL CHECK (comparison IN ('gt', 'ge', 'lt', 'le')),
    threshold double precision NOT NULL,
    PRIMARY KEY (rule_id, feature_name)
);

CREATE TABLE IF NOT EXISTS detection.alerts (
    alert_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id uuid NOT NULL REFERENCES detection.alert_rules (rule_id),
    merchant_id text REFERENCES ref.merchants (merchant_id),
    card_token text,  -- HMAC token (ADR-009); raw PANs never reach the database
    event_id uuid NOT NULL,
    -- Point-in-time copy of the values that fired the rule. Write-once, shape
    -- varies per rule, never queried by its internals — a documented jsonb use
    -- (docs/db/data-dictionary.md).
    feature_snapshot jsonb NOT NULL,
    status text NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'acknowledged', 'resolved')),
    created_at timestamptz NOT NULL DEFAULT now(),
    acknowledged_by text,
    acknowledged_at timestamptz,
    resolved_by text,
    resolved_at timestamptz,
    CHECK (
        (merchant_id IS NOT NULL AND card_token IS NULL)
        OR (merchant_id IS NULL AND card_token IS NOT NULL)
    ),
    CHECK ((status = 'resolved') = (resolved_at IS NOT NULL))
);

-- Integrity lives in the schema, not in consumer code: at most one open alert
-- per (rule, subject), race-free under concurrent consumers.
CREATE UNIQUE INDEX IF NOT EXISTS alerts_one_open_card
    ON detection.alerts (rule_id, card_token)
    WHERE status = 'open' AND card_token IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS alerts_one_open_merchant
    ON detection.alerts (rule_id, merchant_id)
    WHERE status = 'open' AND merchant_id IS NOT NULL;

-- History reads (dashboard, thesis experiments) scan per rule, newest first.
CREATE INDEX IF NOT EXISTS alerts_rule_created_idx
    ON detection.alerts (rule_id, created_at DESC);

CREATE OR REPLACE VIEW detection.open_alerts_v AS
SELECT
    alert_id,
    rule_id,
    merchant_id,
    card_token,
    event_id,
    feature_snapshot,
    status,
    created_at,
    resolved_at,
    acknowledged_at
FROM detection.alerts
WHERE status IN ('open', 'acknowledged');

-- Audit every alert status transition. SECURITY DEFINER: the function runs as the
-- schema owner, so services need no direct write grant on the audit table.
CREATE OR REPLACE FUNCTION detection.log_alert_status()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO ops.audit_log (actor, action, alert_id, detail)
        VALUES (
            COALESCE(NEW.resolved_by, NEW.acknowledged_by),
            NEW.status,
            NEW.alert_id,
            jsonb_build_object(
                'from', OLD.status,
                'to', NEW.status,
                'rule_id', NEW.rule_id
            )
        );
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS alert_status_audit ON detection.alerts;
CREATE TRIGGER alert_status_audit
    AFTER UPDATE OF status ON detection.alerts
    FOR EACH ROW
    EXECUTE FUNCTION detection.log_alert_status();

-- ---------------------------------------------------------------------------
-- Grants: least privilege per service account (roles created in 00-roles.sh)
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA ingestion TO svc_ingestion;
GRANT SELECT, INSERT, DELETE ON ingestion.idempotency_keys TO svc_ingestion;
GRANT EXECUTE ON FUNCTION ingestion.delete_expired_idempotency_keys() TO svc_ingestion;

GRANT USAGE ON SCHEMA features TO svc_features, svc_gateway, svc_detection;
GRANT SELECT, INSERT, UPDATE
    ON features.feature_definitions, features.feature_versions
    TO svc_features;
GRANT SELECT
    ON features.feature_definitions, features.feature_versions
    TO svc_gateway, svc_detection;

GRANT USAGE ON SCHEMA detection, ref TO svc_detection;
GRANT SELECT
    ON detection.alert_rules, detection.rule_features, detection.open_alerts_v
    TO svc_detection;
GRANT SELECT, INSERT, UPDATE ON detection.alerts TO svc_detection;
GRANT SELECT, INSERT ON ref.merchants TO svc_detection;

GRANT USAGE ON SCHEMA detection, ref, ops TO svc_gateway;
GRANT SELECT
    ON detection.open_alerts_v, detection.alerts, detection.alert_rules,
       ref.merchants, ops.operators_masked
    TO svc_gateway;
