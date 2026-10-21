---
title: Setting up a MySQL external source (2-NEXT+)
---

In this guide we will set up a [MySQL external source](/ops/external-sources/mysql/reference) that mirrors the `users` and `orders` tables of the MySQL database `shop` into a database in XTDB called `mysql_shop`.

To do so we will:

1. Check the binlog settings on MySQL
1. Create a user with the appropriate privileges on MySQL
1. Size binlog retention on MySQL
1. Configure MySQL credentials on the XTDB node and redeploy
1. Run `ATTACH DATABASE` in XTDB

## Prerequisites

As with other [external sources](/ops/external-sources/overview) you will need:

- A [transaction log](/ops/config/log)
- An [object store](/ops/config/storage)

:::caution
Ensure that you have a transaction log and object store that do not conflict with other databases.
:::

Additionally you will need MySQL 8.0.20 or later, and tables set up for XTDB — each with an `_id` column, see [DirectMirror](/ops/external-sources/mysql/reference#directmirror).

## Check the binlog settings

The source reads MySQL's [binary log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html) in row format, and resumes from a [GTID](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html) set.
It checks these settings whenever it starts, and refuses to start without them:

```ini
[mysqld]
server-id = 1
log_bin = binlog
binlog_format = ROW
binlog_row_image = FULL
binlog_row_metadata = FULL
gtid_mode = ON
enforce_gtid_consistency = ON
```

`binlog_row_metadata = FULL` is the one most often missing — it is off by default, and without it the binlog carries no column names.
`binlog_transaction_compression` must be left `OFF`.

## Create the MySQL user

You will need a user with the privileges from [here](/ops/external-sources/mysql/reference#prerequisites):

```sql
CREATE USER 'xtdb'@'%' IDENTIFIED BY 'changeme';
GRANT SELECT ON shop.* TO 'xtdb'@'%';
GRANT RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'xtdb'@'%';
```

## Size binlog retention

MySQL purges binlog files on a timer of its own (`binlog_expire_logs_seconds`, default 30 days) — nothing XTDB does holds them back.
After a node loses its local state, XTDB resumes from the last block it has written to object storage, so MySQL must still hold the binlog since then.

Two [node settings](/ops/config) drive that block cadence, whichever comes first:

`rowsPerBlock` (default `102400`)

: A busy database reaches this long before any timeout.

`flushDuration` (default `PT15M`)

: A quiet database cuts a block on this timer instead.

So retention has to cover `flushDuration` plus however long XTDB may be down — the default of 30 days leaves ample room.
If XTDB comes back to find the binlog it needs purged, it refuses to resume rather than skip transactions — see [troubleshooting](/ops/external-sources/mysql/troubleshooting#the-upstream-has-purged-binlog-xtdb-still-needs).

## Deploy the MySQL credentials

Configured under the [`remotes`](/ops/config#remotes) section of the node config like so:

```yaml
remotes:
  mysql_remote: !MySQL
    hostname: mysql_hostname
    port: 3306
    username: !Env MYSQL_USER
    password: !Env MYSQL_PASSWORD
```

For nodes to pick up this config change a rolling re-deploy is required.

## Run `ATTACH DATABASE` in XTDB

Finally to attach the secondary database with the external source:

```sql
ATTACH DATABASE mysql_shop WITH $$
# Use what you set up in the prerequisites
log: !Local
  path: 'mysql_shop/log'
storage: !Local
  path: 'mysql_shop/storage'

externalSource: !MySQL
  remote: mysql_remote
  serverId: 5701
  tables: [shop.users, shop.orders]
$$
```

`serverId` identifies XTDB to MySQL as a replica, so it must differ from the server's own `server-id` and from every other replica's.

You can now query the database by connecting to the `mysql_shop` database and running:
```sql
SELECT * FROM shop.users;
```

Or from another database in XTDB by running:
```sql
SELECT * FROM mysql_shop.shop.users;
```

If you have any problems, please see the [troubleshooting](/ops/external-sources/mysql/troubleshooting) guide.
