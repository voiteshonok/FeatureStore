-- Operator authentication for the dashboard (T7.3 / ADR-020).
-- Applied after 40-alerts-ui.sql on fresh volumes.

-- Gateway may read only the columns needed to verify a password and issue a JWT.
GRANT SELECT (operator_id, email, password_hash, role) ON ops.operators TO svc_gateway;

-- Viewer seed for 403 demos on PATCH (also in 20-seed.sql for fresh installs).
INSERT INTO ops.operators (email, password_hash, role)
VALUES
    ('viewer@featurestore.local', crypt('demo-viewer-pass', gen_salt('bf')), 'viewer')
ON CONFLICT (email) DO NOTHING;
