# Infrastructure

`postgres/00-roles.sh`, `10-schema.sql`, and `20-seed.sql` run only when the Postgres data volume is created, in that order. After changing them, run `make clean && make up`.

The data dictionary is [docs/db/data-dictionary.md](../docs/db/data-dictionary.md). The criterion write-up is [docs/criteria/database.md](../docs/criteria/database.md).
