---
title: Troubleshooting a Postgres external source
---

If a source's ingestion has stopped, you can make the database dormant — see [Skipping Databases](/ops/troubleshooting#skipping-databases-v22) in the general troubleshooting guide.

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
