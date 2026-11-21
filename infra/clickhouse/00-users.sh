#!/bin/bash
# Per-service ClickHouse login accounts.
# Passwords come from the environment (infra/.env, never committed), with dev-only
# defaults for local compose. Runs once on first container start, before 10-schema.sql.
# Requires CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 on the admin user.
set -euo pipefail

clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery <<-SQL
    -- Per-service accounts. The application never connects as ${CLICKHOUSE_USER}
    -- (schema owner / init only).
    CREATE USER IF NOT EXISTS svc_offline IDENTIFIED BY '${SVC_OFFLINE_PASSWORD:-svc_offline_dev}';
    CREATE USER IF NOT EXISTS svc_features_read IDENTIFIED BY '${SVC_FEATURES_READ_PASSWORD:-svc_features_read_dev}';
SQL
