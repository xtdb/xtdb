---
title: MySQL External Source (2-NEXT+)
---

## Prerequisites

MySQL 8.0.20 or later, writing a binlog the source can read — it checks these on start, and names the one that's wrong:

| Setting | Required value |
| --- | --- |
| `log_bin` | on |
| `binlog_format` | `ROW` |
| `binlog_row_image` | `FULL` |
| `binlog_row_metadata` | `FULL` |
| `gtid_mode` | `ON` |
| `binlog_row_value_options` | empty |
| `binlog_transaction_compression` | `OFF` |

`binlog_format`, `binlog_row_image` and `binlog_row_value_options` can also be set per session, so a client can still write a change the source can't read after it has started.
When one arrives, the source stops at that transaction rather than skip it (see [troubleshooting](/ops/external-sources/mysql/troubleshooting)).

A [user](https://dev.mysql.com/doc/refman/8.0/en/create-user.html) with the following [privileges](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html):
- `SELECT` on every included table. Used during snapshotting.
- `RELOAD`. The snapshot holds `FLUSH TABLES WITH READ LOCK` for as long as it takes to open a consistent read and record the GTID set it is at.
- `REPLICATION SLAVE`. Allows reading the binlog.
- `REPLICATION CLIENT`. Allows reading the server's binlog position.

## Configuration

Configured by [attaching a secondary database](/about/dbs-in-xtdb#attachingdetaching-secondary-databases-v21) with the additional `externalSource` options.
It reads from MySQL via a [`!MySQL` remote](#remote):

```sql
ATTACH DATABASE my_db WITH $$
externalSource: !MySQL
  # The alias of the !MySQL remote holding the connection details.
  remote: my_mysql_remote

  # The id this source replicates from MySQL under, between 1 and 4294967295.
  # It must differ from the server's own server-id, and from every other replica of the same server.
  serverId: 5701

  # The tables to mirror, as 'database.table', named exactly as MySQL stores them.
  # This one list drives both the snapshot and the binlog stream.
  tables: [shop.users, shop.orders]
$$
```

### Remote

MySQL connection details are stored in a `!MySQL` [remote](/ops/config#remotes):

```yaml
remotes:
  my_mysql_remote: !MySQL
    # The hostname of the MySQL to connect to.
    hostname: my.mysql.host

    # The port of the MySQL to connect to. Defaults to 3306.
    port: 3307

    # The username of the user to authenticate with MySQL.
    username: xtdb

    # The password for the user connecting to MySQL.
    password: !Env MYSQL_PASSWORD
```

## Phases

This external source works in two phases:

snapshot
:   The initial mode of a MySQL External Source.
    Reads every row of the included tables into XTDB, as one consistent read.
    :::caution
    If interrupted, snapshotting can fail requiring a DETACH then re-ATTACH
    :::

streaming
:   Translates transactions from the MySQL binlog into XTDB transactions — one XTDB transaction per MySQL transaction, all of its rows across all the included tables.
    A MySQL transaction that touches none of the included tables, or only changes schema, still becomes an XTDB transaction, empty, so that XTDB's position in the binlog keeps up with MySQL.

Changes to a table's columns are picked up row by row, so an `ALTER TABLE ... ADD COLUMN` on an included table needs nothing further.
No other DDL is replicated.

## System Time

The system time of rows depends on the phase of the MySQL External Source.

| Phase | System Time |
| --- | --- |
| Snapshot | XTDB's internal system time (i.e. it is unrelated to the upstream MySQL's clock) |
| Streaming | The commit time of the MySQL transaction, to the microsecond — on a replica, when it first committed on its original server |

## Types

Each MySQL column lands as the same XTDB value whether the snapshot or the binlog read it.

| MySQL | XTDB |
| --- | --- |
| `TINYINT` to `BIGINT`, signed or unsigned, and `BOOLEAN` | 64-bit integer |
| `BIGINT UNSIGNED` | decimal |
| `FLOAT` / `DOUBLE` | 32-bit / 64-bit float |
| `DECIMAL` | decimal |
| `CHAR`, `VARCHAR`, `TEXT` | string |
| `BINARY`, `VARBINARY`, `BLOB` | bytes |
| `BIT(1)` | boolean |
| `BIT(n)` | bytes, most significant first |
| `ENUM` | string |
| `SET` | set of strings |
| `DATE` | date |
| `DATETIME` | timestamp without time zone |
| `TIMESTAMP` | timestamp with time zone, in UTC |
| `TIME` | duration (MySQL's `TIME` ranges over ±838 hours) |
| `YEAR` | 64-bit integer |
| `JSON` | the document as XTDB maps, lists and scalars |
| `GEOMETRY` and its subtypes | bytes, in MySQL's internal format |

MySQL's zero dates (`'0000-00-00'` and the like), and a zero `YEAR`, land as null.

## DirectMirror

The source mirrors each row into the XTDB table of the same name, in the schema named after its MySQL database — `shop.users` lands as `shop.users`.

Additional properties are required from the included tables:
- `_id` is a required column, used as the primary key for the row
  - An `UPDATE` that changes `_id` ends the row under the old `_id`
- If set, the `_valid_from` and `_valid_to` columns must be of type `TIMESTAMP` — a `DATETIME` is in no time zone, so isn't an instant
  - Like in XTDB, it is incorrect to specify a `_valid_to` without a `_valid_from`
