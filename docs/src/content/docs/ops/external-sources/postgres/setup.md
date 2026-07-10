---
title: Setting up a Postgres external source
---

In this guide we will set up a [Postgres external source](/ops/external-sources/postgres/reference) from the Postgres database `test_db` to sync all tables in the `public` schema into a database in XTDB called `pg_test_db`.

To do so we will:

1. Create a role with the appropriate permissions on Postgres
1. Create a publication on Postgres
1. Configure Postgres credentials on the XTDB node and redeploy
1. Run `ATTACH DATABASE` in XTDB

## Prerequisites

As with other [external sources](/ops/external-sources/overview) you will need:

- A [transaction log](/ops/config/log)
- An [object store](/ops/config/storage)

:::caution
Ensure that you have a transaction log and object store that do not conflict with other databases.
:::

Additionally you will need to ensure that Postgres is configured with [`wal_level=logical`](https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-WAL-LEVEL).

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
