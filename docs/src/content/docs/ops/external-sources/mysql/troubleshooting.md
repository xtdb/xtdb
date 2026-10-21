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
