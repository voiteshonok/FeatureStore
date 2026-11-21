-- Alerts UI support for the operator dashboard (T7.1 / ADR-019).
-- Applied after 10-schema.sql / 20-seed.sql / 30-baseline.sql on fresh volumes.

ALTER TABLE detection.alerts
    ADD COLUMN IF NOT EXISTS resolution_note text;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'alerts_resolution_note_len'
          AND conrelid = 'detection.alerts'::regclass
    ) THEN
        ALTER TABLE detection.alerts
            ADD CONSTRAINT alerts_resolution_note_len
            CHECK (
                resolution_note IS NULL
                OR (
                    char_length(resolution_note) BETWEEN 3 AND 500
                )
            );
    END IF;
END
$$;

-- Keyset pagination for dashboard list (newest / oldest first).
CREATE INDEX IF NOT EXISTS alerts_created_id_idx
    ON detection.alerts (created_at DESC, alert_id DESC);

-- Status filter + keyset: listing by status scans this rather than the full
-- history index (alerts_rule_created_idx), which is rule-scoped.
CREATE INDEX IF NOT EXISTS alerts_status_created_idx
    ON detection.alerts (status, created_at DESC, alert_id DESC);

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
                'rule_id', NEW.rule_id,
                'resolution_note', NEW.resolution_note
            )
        );
    END IF;
    RETURN NEW;
END;
$$;

GRANT USAGE ON SCHEMA ops TO svc_detection;
GRANT SELECT ON ops.audit_log TO svc_detection;
