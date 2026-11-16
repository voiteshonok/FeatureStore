#!/bin/bash
# Group roles and per-service login accounts.
# Passwords come from the environment (infra/.env, never committed), with dev-only
# defaults for local compose. Runs once on first container start, before 10-schema.sql.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    -- Access tiers (no login).
    CREATE ROLE fs_read  NOLOGIN;
    CREATE ROLE fs_write NOLOGIN;
    CREATE ROLE fs_admin NOLOGIN CREATEROLE;

    -- Per-service accounts. The application never connects as ${POSTGRES_USER}
    -- (schema owner, migrations only) and never as a superuser.
    CREATE ROLE svc_ingestion LOGIN PASSWORD '${SVC_INGESTION_PASSWORD:-svc_ingestion_dev}';
    CREATE ROLE svc_features  LOGIN PASSWORD '${SVC_FEATURES_PASSWORD:-svc_features_dev}';
    CREATE ROLE svc_detection LOGIN PASSWORD '${SVC_DETECTION_PASSWORD:-svc_detection_dev}';
    CREATE ROLE svc_gateway   LOGIN PASSWORD '${SVC_GATEWAY_PASSWORD:-svc_gateway_dev}';
    -- Measurement stand (ADR-015). Not a product service and not a member of the tiers above.
    CREATE ROLE svc_baseline  LOGIN PASSWORD '${SVC_BASELINE_PASSWORD:-svc_baseline_dev}';

    GRANT fs_write TO svc_ingestion, svc_features, svc_detection;
    GRANT fs_read  TO svc_gateway;
SQL
