---
title: Transactions/Consistency in XTDB
---

This article describes how transactions and consistency work in XTDB.

Being a 'log-centric database', it differs from traditional RDBMSs in a couple of key ways:

1. Transactions are non-interactive - either read-only or mutating.

   When you submit a transaction to XTDB, internally, it wraps up the transaction operations and sends them as an atomic message on a shared log.
   
   Within a transaction, your operations can still read the current state of the database (e.g. `UPDATE table SET version = version + 1 WHERE _id = ?`) - just that you can't mix read queries like `SELECT` which return results and mutations like `UPDATE` or `DELETE` in the same transaction.
   
   (If you do need this behaviour, see [`ASSERT`](/reference/main/sql/txs#assert))

2. All mutable transactions are serialized via a totally-ordered log.

   This means that XT, internally, has very little locking - this gives us excellent per-thread performance, as well as removing a whole category of bugs/errors.

In more detail, XTDB is heavily inspired by the 'Epochal Time Model', coined in Clojure and Datomic author Rich Hickey's talk 'Database as a Value'[^1]:
[^1]: https://www.infoq.com/presentations/database-value/

![Epochal Time Model](/images/docs/epochal-time-model.webp)

## Transaction consistency:

Mutable transactions in XTDB are serialized via a totally-ordered log - these are then indexed in order, one at a time, on each of the nodes.
As a result, they are trivially consistent to the highest isolation level - 'serializable'.

On the read-side, within a connection, queries are guaranteed to at least see the results of every transaction submitted through that connection.
Therefore, if you're looking to reliably write-then-read, the easiest way to do this is to re-use the same connection for your write and subsequent read.

Between connections, by default, you may not see the results of transactions submitted on other connections immediately - you likely will, but it's not guaranteed.
To ensure that you do see the results of transactions on other connections, XTDB provides an `AWAIT_TOKEN` variable, which you can pass from one connection to another:

```sql
-- on connection A:
BEGIN; -- BEGIN/COMMIT optional
UPDATE ...;
COMMIT;

SHOW AWAIT_TOKEN;
-- "CgsKBHh0ZGISAwoBAQ=="

-- on connection B:

BEGIN READ ONLY WITH (AWAIT_TOKEN = 'CgsKBHh0ZGISAwoBAQ==');
SELECT ...; -- will see the results of the transaction on connection A
COMMIT;

-- or, through `SET`
SET AWAIT_TOKEN = 'CgsKBHh0ZGISAwoBAQ==';
```

This await-token represents a lower-bound of the transactions that must be available on the queried node before the queries are evaluated.

## Repeatable queries - 'basis':

Every read-only query in XTDB is evaluated at a 'basis' - both a 'snapshot' of the database, which determines the transactions visible to the query, and a 'clock time'.

For more details, see the ['basis' reference documentation](/reference/main/sql/queries#basis).

### Snapshots:

No matter what else is going on in the database at the time, your query will only see a consistent state of the database as of that precise snapshot.
This snapshot is fixed at the start of your transaction - all queries within a given transaction use the same snapshot.

It defaults to including all of the processed transactions on the queried node at the start of the transaction - but (advanced) you can also explicitly specify a 'snapshot token', either as part of your `BEGIN` statement, or at the start of a specific query.

  ```sql
  -- retrieve a snapshot token from an earlier transaction

  BEGIN;

  SELECT ...;
  -- ...
  
  SHOW SNAPSHOT_TOKEN;
  -- "ChYKBHh0ZGISDgoMCKHqs8cGEPCP2poB"

  COMMIT;
  

  -- then, use that token in a later transaction to use the same snapshot:

  BEGIN READ ONLY 
    WITH (SNAPSHOT_TOKEN = 'ChYKBHh0ZGISDgoMCKHqs8cGEPCP2poB');
    
  -- run the same query again - it will return the same results as before:
  SELECT ...;
  
  -- alternatively, on a per-query basis:
  SETTING SNAPSHOT_TOKEN = 'ChYKBHh0ZGISDgoMCKHqs8cGEPCP2poB'
  SELECT ...;
  ```
  
Snapshots are an *upper-bound* on the transactions visible to your query.
  
### Clock time

XTDB also supports setting the clock time, both at transaction and query level.
Similarly to the snapshot token, this is fixed at the start of your transaction, but may be overridden on a per-query basis.

```sql
-- at the start of a transaction:
BEGIN READ ONLY 
  WITH (CLOCK_TIME = TIMESTAMP '2023-01-01Z');
  
-- on a per-query basis:
SETTING CLOCK_TIME = TIMESTAMP '2023-01-01Z'
SELECT ...;
```

`CLOCK_TIME` has a couple of effects:

- Any call to `CURRENT_TIMESTAMP` et al will return the set time.
- Any tables in `FROM` clauses that don't otherwise have a valid-time specification will be resolved with respect to that time.

So, for full query repeatability, you should set both `SNAPSHOT_TOKEN` and `CLOCK_TIME`.
