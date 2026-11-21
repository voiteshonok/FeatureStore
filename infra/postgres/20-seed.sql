-- Reference and demo data. Runs once, after 10-schema.sql.

-- Minimal registry rows so the demo rules below can reference features before the
-- features api performs its startup upsert of the full catalog (ADR-012).
INSERT INTO features.feature_definitions (name, entity, kind, agg, default_value)
VALUES
    ('card_cnt_1m', 'card', 'stream', 'count', 0),
    ('geo_velocity_kmh', 'card', 'on_demand', 'geo_velocity', NULL),
    ('merchant_decline_rate_24h', 'merchant', 'stream', 'decline_rate', 0),
    ('merchant_cnt_24h', 'merchant', 'stream', 'count', 0)
ON CONFLICT (name) DO NOTHING;

-- Demo attributes for the first simulator merchants (ids are m0…mN, see
-- simulator/src/simulator/population.py). Reference-table fill demo; the rest of
-- the population is registered by detection on first sight with defaults.
INSERT INTO ref.merchants (merchant_id, display_name, category, city, risk_level)
VALUES
    ('m0', 'Baltic Grocery',        'grocery',       'Vilnius', 'low'),
    ('m1', 'Kaunas Electronics',    'electronics',   'Kaunas',  'medium'),
    ('m2', 'Riga Fuel 24',          'fuel',          'Riga',    'low'),
    ('m3', 'NightLux Club',         'entertainment', 'Vilnius', 'high'),
    ('m4', 'TravelNow OTA',         'travel',        NULL,      'medium'),
    ('m5', 'Quick Loans Online',    'financial',     NULL,      'high'),
    ('m6', 'Tallinn Pharmacy',      'pharmacy',      'Tallinn', 'low'),
    ('m7', 'GameTop Digital Goods', 'digital_goods', NULL,      'medium')
ON CONFLICT (merchant_id) DO NOTHING;

-- Dashboard operators. Passwords exist only as bcrypt hashes (pgcrypto); these
-- demo credentials are for the local environment.
INSERT INTO ops.operators (email, password_hash, role)
VALUES
    ('viewer@featurestore.local',  crypt('demo-viewer-pass',  gen_salt('bf')), 'viewer'),
    ('analyst@featurestore.local', crypt('demo-analyst-pass', gen_salt('bf')), 'analyst'),
    ('admin@featurestore.local',   crypt('demo-admin-pass',   gen_salt('bf')), 'admin')
ON CONFLICT (email) DO NOTHING;

-- Demo alert rules. impossible_travel stays disabled: snapshots carry stream
-- features only (ADR-013), on-demand rules are skipped by detection.
INSERT INTO detection.alert_rules (rule_id, name, entity, enabled)
VALUES
    ('c0ffee00-0000-4000-8000-000000000001', 'card_velocity_burst', 'card', true),
    ('c0ffee00-0000-4000-8000-000000000002', 'impossible_travel', 'card', false),
    ('c0ffee00-0000-4000-8000-000000000003', 'merchant_decline_wave', 'merchant', true)
ON CONFLICT (name) DO NOTHING;

INSERT INTO detection.rule_features (rule_id, feature_name, comparison, threshold)
VALUES
    ('c0ffee00-0000-4000-8000-000000000001', 'card_cnt_1m', 'gt', 5),
    ('c0ffee00-0000-4000-8000-000000000002', 'geo_velocity_kmh', 'gt', 900),
    ('c0ffee00-0000-4000-8000-000000000003', 'merchant_decline_rate_24h', 'gt', 0.35),
    ('c0ffee00-0000-4000-8000-000000000003', 'merchant_cnt_24h', 'ge', 20)
ON CONFLICT (rule_id, feature_name) DO NOTHING;
