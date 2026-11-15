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

-- Read-only feature registry for the gateway (ADR-008).
-- The password matches the other service roles.
CREATE ROLE svc_gateway LOGIN PASSWORD 'featurestore';

GRANT USAGE ON SCHEMA features TO svc_gateway;
GRANT SELECT
    ON features.feature_definitions, features.feature_versions
    TO svc_gateway;

-- Detection rules and alerts. svc_detection reads feature kind from the catalog
-- table and does not read feature values. The password matches svc_features.
-- impossible_travel stays disabled: snapshots carry stream features only (ADR-013).
CREATE ROLE svc_detection LOGIN PASSWORD 'featurestore';

CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS detection;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ref.merchants (
    merchant_id text PRIMARY KEY
);

INSERT INTO features.feature_definitions (name, entity, kind, agg, default_value)
VALUES
    ('card_cnt_1m', 'card', 'stream', 'count', 0),
    ('geo_velocity_kmh', 'card', 'on_demand', 'geo_velocity', NULL)
ON CONFLICT (name) DO NOTHING;

CREATE TABLE IF NOT EXISTS detection.alert_rules (
    rule_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL UNIQUE,
    entity text NOT NULL CHECK (entity IN ('card', 'merchant')),
    enabled boolean NOT NULL DEFAULT true
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
    card_token text,
    event_id uuid NOT NULL,
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
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS alerts_one_open_card
    ON detection.alerts (rule_id, card_token)
    WHERE status = 'open' AND card_token IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS alerts_one_open_merchant
    ON detection.alerts (rule_id, merchant_id)
    WHERE status = 'open' AND merchant_id IS NOT NULL;

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

CREATE TABLE IF NOT EXISTS ops.audit_log (
    audit_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    actor text,
    action text NOT NULL,
    alert_id uuid NOT NULL,
    detail jsonb NOT NULL
);

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

CREATE TRIGGER alert_status_audit
    AFTER UPDATE OF status ON detection.alerts
    FOR EACH ROW
    EXECUTE FUNCTION detection.log_alert_status();

GRANT USAGE ON SCHEMA detection, ref, features TO svc_detection;
GRANT SELECT
    ON detection.alert_rules, detection.rule_features, detection.open_alerts_v,
       features.feature_definitions
    TO svc_detection;
GRANT SELECT, INSERT, UPDATE ON detection.alerts TO svc_detection;
GRANT SELECT, INSERT ON ref.merchants TO svc_detection;

INSERT INTO detection.alert_rules (rule_id, name, entity, enabled)
VALUES
    ('c0ffee00-0000-4000-8000-000000000001', 'card_velocity_burst', 'card', true),
    ('c0ffee00-0000-4000-8000-000000000002', 'impossible_travel', 'card', false)
ON CONFLICT (name) DO NOTHING;

INSERT INTO detection.rule_features (rule_id, feature_name, comparison, threshold)
VALUES
    ('c0ffee00-0000-4000-8000-000000000001', 'card_cnt_1m', 'gt', 5),
    ('c0ffee00-0000-4000-8000-000000000002', 'geo_velocity_kmh', 'gt', 900)
ON CONFLICT (rule_id, feature_name) DO NOTHING;
