# Infrastructure

`postgres/00-roles.sh`, `10-schema.sql`, `20-seed.sql`, `30-baseline.sql`, `40-alerts-ui.sql`, and `50-operator-auth.sql` run only when the Postgres data volume is created, in that order. `30-baseline.sql` is the SQL measurement stand, not a product schema (ADR-015). `40-alerts-ui.sql` adds dashboard columns/indexes and `ops.audit_log` read grant for detection (ADR-019). `50-operator-auth.sql` grants gateway column SELECT on `ops.operators` and seeds the viewer operator (ADR-020).

`clickhouse/00-users.sh` and `10-schema.sql` run only when the ClickHouse data volume is created, in that order (service users then DDL + grants). After changing either set of scripts, run `make clean && make up`.

The data dictionary is [docs/db/data-dictionary.md](../docs/db/data-dictionary.md). The criterion write-up is [docs/criteria/database.md](../docs/criteria/database.md).
