---
title: Troubleshooting a Postgres external source
---

If a source's ingestion has stopped, you can make the database dormant — see [Skipping Databases](/ops/troubleshooting#skipping-databases-v22) in the general troubleshooting guide.

## Ingestion won't start on PostgreSQL 16 or earlier

**Symptom:**
Ingestion never starts, and the database reports an error containing `unrecognized option: failover`.

**Cause:**
XTDB creates its replication slot with the `failover` option, which was introduced in PostgreSQL 17.
Earlier versions reject the whole command, so no slot is created and nothing is left behind on the upstream.

**Resolution:**
Upgrade the upstream to PostgreSQL 17 or later.
There is no configuration that makes an earlier version work — see [prerequisites](/ops/external-sources/postgres/reference#prerequisites).

If you need to run against an earlier version, please [raise an issue](https://github.com/xtdb/xtdb/issues/new) — supporting one is something we would consider.

## Ingestion won't start against a standby

**Symptom:**
Ingestion never starts, and the database reports an error containing `cannot enable failover for a replication slot created on the standby`.

**Cause:**
The source is pointed at a standby rather than a primary.
Postgres allows logical decoding from a standby, but rejects the `failover` option on a slot created there, and XTDB always sets it.

**Resolution:**
Point the source at the primary.
If you were reading from a standby to keep decoding load off the primary, note that this is not currently supported.

## Recovering from a failed initial snapshot

**Symptom:**
Ingestion for the database has stopped with the error `Incomplete snapshot — database is inoperable` (`xtdb.postgres/incomplete-snapshot`).
The initial snapshot was interrupted before it completed. The node lost leadership mid-snapshot, or was restarted.

**Why a reset is needed:**
Snapshotting must complete in a single run because a half-finished snapshot can't be resumed by a later leader.
This should be rare: nodes tend to hold leadership for a long time, and snapshotting is a relatively short period.

**Resolution:**

1. Detach the database in XTDB:
```sql
DETACH DATABASE pg_test_db;
```

2. Delete the slot on Postgres:
```sql
SELECT pg_drop_replication_slot('xtdb');
```

3. Delete the publication on Postgres:
```sql
DROP PUBLICATION xtdb;
```

4. Clear the log:
If using a [kafka log](/ops/config/log/kafka) you can clear the source & replica topics by either deleting and recreating them, or briefly setting the retention period to 1ms.

5. Clear the object store:
Delete everything under the location set in the `storage` block of the `ATTACH` — the `!Local` path, or the bucket and prefix of a remote object store.

6. Re-run the [setup guide](/ops/external-sources/postgres/setup) from the beginning

## The replication slot keeps growing

**Symptom:**
Retained WAL for the XTDB slot grows steadily, while XTDB itself looks healthy — rows are queryable, and `healthz` is green.

```sql
SELECT slot_name, wal_status,
       pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) AS retained
FROM pg_replication_slots WHERE slot_name = 'xtdb';
```

The same figure is exported as the `xtdb.postgres_source.wal_lag_bytes` gauge.

**Why this happens:**
Where a transaction has been indexed but the block carrying it is not yet in object storage, Postgres is the only place it can be re-read from, so XTDB holds `confirmed_flush_lsn` at the last durable block and the WAL behind it stays.
Retention therefore builds up over a block, and falls back to nothing each time one lands.

That makes a growing slot a signal about *blocks*, not about ingestion.
Two causes, distinguished by whether the block index is advancing:

**Resolution:**

1. Check when this database last cut a block, via the `xtdb.block.last_upload_time` gauge — epoch seconds, tagged `db`.
   A value that stops advancing while the slot keeps growing is the signal to act on.

2. If blocks *are* being cut, the retention is the WAL written since the last one — up to `flushDuration`'s worth, four hours on the default.
   This is working as intended.
   Either size retention for it, per [Size WAL retention](/ops/external-sources/postgres/setup#size-wal-retention), or lower `flushDuration` to trade more, smaller blocks for a lower retention floor.

3. If blocks are *not* being cut, block flushing is stuck and the slot will grow without bound.
   Check the node logs for object-store write failures, and check that some node holds leadership for this database.

:::caution
Do not confirm the slot by hand with `pg_replication_slot_advance` to reclaim space.
That tells Postgres to discard WAL that XTDB has not persisted, and any transaction in the discarded range is lost — it exists only in the replica log and the leader's in-memory index, and cannot be re-sent.
:::

## Ingestion halted on an unchanged TOASTed column

**Symptom:**
Ingestion has stopped with an error like `Received unchanged TOASTed column '<column>' on <schema>.<table>`.

**Cause:**
The table isn't set to `REPLICA IDENTITY FULL`.
When an `UPDATE` leaves a large ([TOASTed](https://www.postgresql.org/docs/current/storage-toast.html)) column unchanged, Postgres omits its value from the replication stream.
XTDB mirrors the whole row, so without that value it can't reconstruct the row and halts.

**Resolution:**
Set [`REPLICA IDENTITY FULL`](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-ALTERTABLE-REPLICA-IDENTITY) on the table, so the unchanged value is carried in the old tuple of each change:
```sql
ALTER TABLE "public"."my_table" REPLICA IDENTITY FULL;
```
This only affects changes written after it's set.
A source that has already halted won't resume on its own — the change that stopped it was already written without the value — so reset it as in [Recovering from a failed initial snapshot](#recovering-from-a-failed-initial-snapshot) once `REPLICA IDENTITY FULL` is in place.
