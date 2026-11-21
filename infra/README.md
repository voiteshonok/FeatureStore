# Infrastructure

`postgres/00-roles.sh`, `10-schema.sql`, `20-seed.sql`, and `30-baseline.sql` run only when the Postgres data volume is created, in that order. `30-baseline.sql` is the SQL measurement stand, not a product schema (ADR-015).

`clickhouse/00-users.sh` and `10-schema.sql` run only when the ClickHouse data volume is created, in that order (service users then DDL + grants). After changing either set of scripts, run `make clean && make up`.

The data dictionary is [docs/db/data-dictionary.md](../docs/db/data-dictionary.md). The criterion write-up is [docs/criteria/database.md](../docs/criteria/database.md).
