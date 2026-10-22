---
title: Troubleshooting a MySQL external source (2-NEXT+)
---

If a source's ingestion has stopped, you can make the database dormant — see [Skipping Databases](/ops/troubleshooting#skipping-databases-v22) in the general troubleshooting guide.

## Ingestion won't start: the upstream's setting is wrong

**Symptom:**
Ingestion never starts, and the database reports an error `the upstream's <setting> is '<value>'` (`xtdb.mysql/upstream-setting`).

**Cause:**
MySQL isn't writing a binlog the source can read — see the [prerequisites](/ops/external-sources/mysql/reference#prerequisites) for the setting the error names.

**Resolution:**
Change the setting on MySQL and restart it where the setting needs that, then restart the XTDB node.
Nothing was ingested, so nothing needs resetting.

## Ingestion stopped: the binlog carries a change the source can't read

**Symptom:**
Ingestion stops with `xtdb.mysql/upstream-setting` naming `binlog_format`, `binlog_row_image` or `binlog_row_value_options`, or with `xtdb.mysql/compressed-transaction`.

**Cause:**
A client session changed one of those settings for itself, and wrote a transaction the source can't read in full.
The source stops at that transaction rather than skip it.

**Resolution:**
The transaction can't be read again in another form, so the database has to be re-created — see [recovering from a failed initial snapshot](#recovering-from-a-failed-initial-snapshot).
Find and fix the client that sets the session variable first.

## The upstream has purged binlog XTDB still needs

**Symptom:**
Ingestion stops with `the upstream has purged binlog XTDB still needs` (`xtdb.mysql/binlog-purged`).

**Cause:**
MySQL purged binlog files holding transactions XTDB hadn't yet read — XTDB was down, or behind, for longer than `binlog_expire_logs_seconds`.
The source won't resume past the gap, since it would silently miss those transactions.

**Resolution:**
Re-create the database to snapshot afresh — see [recovering from a failed initial snapshot](#recovering-from-a-failed-initial-snapshot) — and [size binlog retention](/ops/external-sources/mysql/setup#size-binlog-retention) so it doesn't recur.

## The upstream hasn't executed everything XTDB has indexed

**Symptom:**
Ingestion stops with `the upstream hasn't executed everything XTDB has indexed from it` (`xtdb.mysql/upstream-behind`).

**Cause:**
The `!MySQL` remote now points at a different server — a replica that lags the old primary, or one that isn't part of the same replication topology at all.
Resuming would read a history that isn't the one XTDB has mirrored.

**Resolution:**
Point the remote back at the server XTDB was replicating from, or at a replica that has caught up with it.
If the history really has changed, re-create the database.

## The upstream refused to serve the binlog

**Symptom:**
Ingestion stops with `the upstream refused to serve the binlog` (`xtdb.mysql/binlog-refused`), carrying MySQL's own message.

**Cause:**
Most often, another replica — another XTDB source, or a MySQL replica — connected with the same server id, and MySQL dropped this one.

**Resolution:**
Give each source a `serverId` of its own, distinct from every MySQL server and replica in the topology, then restart the node.

## Ingestion has paused and the logs repeat `Binlog stream failed`

**Symptom:**
New rows stop reaching XTDB, the database reports no ingestion error, and the node logs a warning like this on every attempt:

```
[mysql_shop] Binlog stream failed (failure 7 since it last made progress); reopening after GTID set '3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5' in 30s
```

**Cause:**
The source lost its binlog connection, or heard nothing from MySQL for three heartbeats, and is retrying it.
It carries on retrying until it reconnects, so if the cause clears by itself — a restart, a failover or a network blip — you don't need to do anything.
The exception attached to the warning says which cause it is: a connection error against the configured host, an authentication error after the user's credentials changed, or `nothing heard from the upstream`.

**Resolution:**

1. Fix the upstream, or the `!MySQL` remote's configuration; the source reconnects on its next attempt.

2. To stop the retries, make the database dormant.
   This needs a node restart; see [Skipping Databases](/ops/troubleshooting#skipping-databases-v22).

## Recovering from a failed initial snapshot

**Symptom:**
Ingestion for the database has stopped with the error `Incomplete snapshot — database is inoperable` (`xtdb.mysql/incomplete-snapshot`).
The initial snapshot was interrupted before it completed. The node lost leadership mid-snapshot, or was restarted.

**Why a reset is needed:**
Snapshotting must complete in a single run because a half-finished snapshot can't be resumed by a later leader.

**Resolution:**

1. Detach the database in XTDB:
```sql
DETACH DATABASE mysql_shop;
```

2. Clear the log:
If using a [kafka log](/ops/config/log/kafka) you can clear the source & replica topics by either deleting and recreating them, or briefly setting the retention period to 1ms.

3. Clear the object store:
Delete everything under the location set in the `storage` block of the `ATTACH` — the `!Local` path, or the bucket and prefix of a remote object store.

4. Re-attach the database — see [setup](/ops/external-sources/mysql/setup#run-attach-database-in-xtdb).

MySQL holds no state for the source, so there is nothing to clean up on the upstream.
