---
title: Postgres External Source
---

## Prerequisites

PostgreSQL 17 or later.

A [role](https://www.postgresql.org/docs/current/user-manag.html) with the following attributes and privileges:
- The [`LOGIN`](https://www.postgresql.org/docs/current/role-attributes.html) attribute. Allows opening a session in Postgres.
- The [`REPLICATION`](https://www.postgresql.org/docs/current/role-attributes.html) attribute. Allows opening a replication-mode connection to Postgres.
- The [`CONNECT`](https://www.postgresql.org/docs/current/ddl-priv.html#DDL-PRIV-CONNECT) privilege on the database being connected to. Allows opening a connection to Postgres.
- The [`USAGE`](https://www.postgresql.org/docs/current/ddl-priv.html#DDL-PRIV-USAGE) privilege on all schemas of tables in the publication. Used during snapshotting.
- The [`SELECT`](https://www.postgresql.org/docs/current/ddl-priv.html#DDL-PRIV-SELECT) privilege on all tables in the publication. Used during snapshotting.

A publication enumerating the tables to sync to XTDB.
Its tables are synced, less any named in the source's [`ignoreTables`](#ignored-tables-v2-next).

[`REPLICA IDENTITY FULL`](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY) on any table with a column that can hold a large ([TOASTable](https://www.postgresql.org/docs/current/storage-toast.html)) value — `text`, `bytea`, `jsonb`, and the like.
Without it, an `UPDATE` that leaves such a column unchanged omits its value from the replication stream, and — since XTDB mirrors the whole row — the source can't reconstruct the row and halts (see [troubleshooting](/ops/external-sources/postgres/troubleshooting)).

:::caution
Adding non-empty tables to a publication after the initial snapshot is currently unsupported.
Doing so will leave the table in an inconsistent state in XTDB.

This feature is tracked [here](https://github.com/xtdb/xtdb/issues/5497)
:::

If the upstream is an HA pair, see [surviving a Postgres failover](#surviving-a-postgres-failover) for what it needs configured in order for the replication slot to survive a promotion.

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

  # (v2-NEXT+) Tables in the publication to leave out of XTDB (optional, default none).
  ignoreTables: [legacy.events, audit_log]
$$
```

### Ignored tables (v2-NEXT+)

Each `ignoreTables` entry names a table as `schema.table`, or as `table` for a table in `public`.
Names are matched exactly against Postgres's own, so case matters: a table created as `CREATE TABLE Audit_Log` is `audit_log`, and one created as `"Audit_Log"` is `Audit_Log`.

An ignored table is never snapshotted, the values in its replicated inserts, updates and deletes are never converted into rows, and nothing from it reaches the indexer.
So an insert, update or delete that would otherwise stop the source doesn't, such as a row without an `_id`, or an unchanged TOASTed column on a table without `REPLICA IDENTITY FULL`.
A `TRUNCATE` of an ignored table still stops the source, as a `TRUNCATE` of any published table does (tracked [here](https://github.com/xtdb/xtdb/issues/6346)).

The source's configuration is fixed when the database is attached.
To change `ignoreTables`, detach the database and attach it again under the same name, with the same `log`, `storage`, `slotName` and `publicationName` and the new list.
Provided the database had written a block before it was detached, the source resumes from where the database left off, including after a change to a table it couldn't mirror stopped it.

Removing a table from `ignoreTables` doesn't backfill the changes made while it was ignored (tracked [here](https://github.com/xtdb/xtdb/issues/5497)).

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
    password: !Env PG_PASSWORD
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


## Surviving a Postgres failover

XTDB relies on PostgreSQL 17's [slot synchronisation](https://www.postgresql.org/docs/17/logical-replication-failover.html) to keep its replication slot across a failover of the upstream.
It sets `failover` on the slot it creates, which makes that slot eligible to be copied to a standby.

This requires XTDB to be pointed at the primary, and the following configuration options on Postgres:
- On the standby
  - `sync_replication_slots = on`  - run the worker that copies eligible slots
  - `hot_standby_feedback = on` - the primary retains what the copy still needs
- On the primary
  - `synchronized_standby_slots = '<standby's physical slot>'` - to hold decoding back until it confirms receipt

The source's `!Postgres` remote has to point at an address that moves to the new primary, since XTDB reconnects to the address it was configured with.

When the replication connection drops, the source reopens it, and keeps trying until it reconnects.
Ingestion pauses while it retries, and each attempt is logged as a warning.
The database stays queryable.
See [troubleshooting](/ops/external-sources/postgres/troubleshooting#ingestion-has-paused-and-the-logs-repeat-replication-stream-failed) for a source that doesn't come back.


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
