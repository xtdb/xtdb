---
title: Setting up a Postgres external source
---

In this guide we will set up a [Postgres external source](/ops/external-sources/postgres/reference) from the Postgres database `test_db` to sync all tables in the `public` schema into a database in XTDB called `pg_test_db`.

To do so we will:

1. Create a role with the appropriate permissions on Postgres
1. Create a publication on Postgres
1. Size WAL retention on Postgres
1. Configure Postgres credentials on the XTDB node and redeploy
1. Run `ATTACH DATABASE` in XTDB

## Prerequisites

As with other [external sources](/ops/external-sources/overview) you will need:

- A [transaction log](/ops/config/log)
- An [object store](/ops/config/storage)

:::caution
Ensure that you have a transaction log and object store that do not conflict with other databases.
:::

Additionally you will need:

- PostgreSQL 17 or later.
- Postgres configured with [`wal_level=logical`](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-LEVEL).

If the upstream is an HA pair, see [surviving a Postgres failover](/ops/external-sources/postgres/reference#surviving-a-postgres-failover) for what it needs configured in order for the replication slot to survive a promotion.

## Create the Postgres role

You will need a [role](https://www.postgresql.org/docs/current/user-manag.html) with the permissions from [here](/ops/external-sources/postgres/reference#prerequisites).

This can be set up with the following commands:
```sql
CREATE ROLE my_role WITH LOGIN REPLICATION PASSWORD 'changeme';
GRANT CONNECT ON DATABASE test_db TO my_role;
GRANT USAGE ON SCHEMA public TO my_role;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO my_role;
```

## Create the publication

Please use the publication to filter the tables or schemas that you want to sync to XTDB, for example:
```sql
CREATE PUBLICATION xtdb
-- FOR ALL TABLES
-- FOR TABLE test_table
FOR TABLES IN SCHEMA public;
```

:::caution[Don't add tables with existing data after attaching]
Adding a non-empty table to the publication after attaching leaves it in an inconsistent state in XTDB.
Rows that existed before the `ALTER PUBLICATION` are never snapshotted, only later changes are captured.

Tracked in [this ticket](https://github.com/xtdb/xtdb/issues/5497)
:::

## Size WAL retention

XTDB creates a replication slot, and Postgres retains WAL for that slot until XTDB confirms it.
XTDB confirms only as far as the last block it has written to object storage, so the WAL that Postgres must retain is sized by XTDB's *block cadence*, not by how quickly it indexes each transaction.

Two [node settings](/ops/config) drive that cadence, whichever comes first:

`rowsPerBlock` (default `102400`)

: A busy database reaches this long before any timeout, so retention tracks throughput.

`flushDuration` (default `PT15M`)

: A quiet database cuts a block on this timer instead.
  On the default it sets the worst case: up to fifteen minutes of WAL.

Size `max_slot_wal_keep_size` to cover the peak WAL rate over that worst case, plus headroom:

```sql
-- e.g. 20 MB/s peak × 15m flushDuration ≈ 18 GB, plus headroom
ALTER SYSTEM SET max_slot_wal_keep_size = '32GB';
SELECT pg_reload_conf();
```

Lowering `flushDuration` lowers the retention floor proportionally, at the cost of more, smaller blocks.

:::caution[Exceeding the limit invalidates the slot]
`max_slot_wal_keep_size` does not apply back-pressure to XTDB — when a slot's retained WAL exceeds it, Postgres invalidates the slot and its `wal_status` becomes `lost`.
The only recovery is to drop the XTDB database and re-attach it, which re-snapshots from scratch.

Leaving it at the default of `-1` means unlimited retention, which trades that failure for the Postgres disk filling up instead.
Either way, monitor the slot — see [troubleshooting](/ops/external-sources/postgres/troubleshooting#the-replication-slot-keeps-growing).
:::

## Deploy the Postgres credentials

Configured under the [`remotes`](/ops/config#remotes) section of the node config like so:

```yaml
remotes:
  pg_remote: !Postgres
    hostname: pg_hostname
    port: 5432
    database: test_db
    username: !Env PGUSER
    password: !Env PGPASSWORD
```

For nodes to pick up this config change a rolling re-deploy is required.

## Run `ATTACH DATABASE` in XTDB

Finally to attach the secondary database with the external source:

```sql
ATTACH DATABASE pg_test_db WITH $$
# Use what you set up in the prerequisites
log: !Local
  path: 'pg_test_db/log'
storage: !Local
  path: 'pg_test_db/storage'

externalSource: !Postgres
  remote: pg_remote
  publicationName: xtdb
  slotName: xtdb
  indexer: !DirectMirror {}
$$
```

Note that XTDB creates and manages a replication slot named `slotName`, streaming the tables in `publicationName`.

You can now query the database by connecting to the `pg_test_db` database and running:
```sql
SELECT * FROM test_table;
```

Or from another database in XTDB by running:
```sql
SELECT * FROM pg_test_db.public.test_table;
```

If you have any problems, please see the [troubleshooting](/ops/external-sources/postgres/troubleshooting) guide.
