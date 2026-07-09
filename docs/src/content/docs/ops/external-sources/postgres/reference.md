---
title: Postgres External Source
---

## Prerequisites

A [role](https://www.postgresql.org/docs/current/user-manag.html) with the following attributes and privileges:
- The [`LOGIN`](https://www.postgresql.org/docs/current/role-attributes.html) attribute. Allows opening a session in Postgres.
- The [`REPLICATION`](https://www.postgresql.org/docs/current/role-attributes.html) attribute. Allows opening a replication-mode connection to Postgres.
- The [`CONNECT`](https://www.postgresql.org/docs/current/ddl-priv.html#DDL-PRIV-CONNECT) privilege on the database being connected to. Allows opening a connection to Postgres.
- The [`USAGE`](https://www.postgresql.org/docs/current/ddl-priv.html#DDL-PRIV-USAGE) privilege on all schemas of tables in the publication. Used during snapshotting.
- The [`SELECT`](https://www.postgresql.org/docs/current/ddl-priv.html#DDL-PRIV-SELECT) privilege on all tables in the publication. Used during snapshotting.

A publication enumerating the tables to sync to XTDB, it's table set is the only filter on what gets synced.

:::caution
Adding non-empty tables to a publication after the initial snapshot is currently unsupported.
Doing so will leave the table in an inconsistent state in XTDB.

This feature is tracked [here](https://github.com/xtdb/xtdb/issues/5497)
:::


## Configuration

Configured by [attaching a secondary database](/about/dbs-in-xtdb#attachingdetaching-secondary-databases-v21) with the additional `externalSource` options.
It reads from a Postgres [publication](https://www.postgresql.org/docs/current/sql-createpublication.html) via a [`!Postgres` remote](#remote), and writes through an [indexer](#indexers):

```sql
ATTACH DATABASE my_db WITH $$
externalSource: !Postgres
  # The alias of the !Postgres remote holding the connection details.
  remote: my_pg_remote

  # The Postgres publication to replicate from.
  publicationName: my_db_to_xtdb

  # The replication slot XTDB creates.
  slotName: my_db_to_xtdb

  # The indexer used to index Postgres transactions.
  indexer: !DirectMirror {}
$$
```

### Remote

Postgres connection details are stored in a `!Postgres` [remote](/ops/config#remotes):

```yaml
remotes:
  my_pg_remote: !Postgres
    # The hostname of the Postgres to connect to.
    hostname: my.pg.host

    # The port of the Postgres to connect to. Defaults to 5432.
    port: 5433

    # The Postgres database to connect to.
    database: my_upstream_db

    # The username of the role to authenticate with Postgres.
    username: my_role

    # The password for the role connecting to Postgres.
    password: !ENV PG_PASSWORD
```

## Phases

This external source works in two phases:

snapshot
:   The initial mode of a Postgres External Source.
    Scans all rows for tables in the publication into XTDB.
    :::caution
    If interrupted, snapshotting can fail requiring a DETACH then re-ATTACH
    :::

streaming
:   Translates transactions from Postgres into XTDB transactions.

## System Time

The system time of rows depends on the phase of the Postgres External Source.

| Phase | System Time |
| --- | --- |
| Snapshot | XTDB's internal system time (i.e. it is unrelated to the upstream Postgres' system clock) |
| Streaming | The timestamp of the Postgres transaction (the timestamp on the `commit` message) |


## Indexers

The following indexers are built into XTDB:
- DirectMirror

### DirectMirror

Mirrors tables & transactions from Postgres directly into XTDB.

Additional properties are required from the tables replicated using this indexer:
- `_id` is a required column, used as the primary key for the row
- If set, the `_valid_from` and `_valid_to` columns must be of type `TIMESTAMPTZ`
  - Like in XTDB, it is incorrect to specify a `_valid_to` without a `_valid_from`

No configuration properties are provided.
