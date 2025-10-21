---
title: Transactions/Consistency in XTDB
---

This article describes how transactions and consistency work in XTDB.

Being a 'log-centric database', it differs from traditional RDBMSs in a couple of key ways:

1. Transactions that perform writes ("[DML](https://en.wikipedia.org/wiki/Data_manipulation_language) transactions") to the database are non-interactive.

   When you submit a transaction containing [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) statements to XTDB, internally, it wraps up the transaction operations and sends them as an atomic message on a shared log.
   
   Within a transaction, your operations can still read the current state of the database (e.g. `UPDATE table SET version = version + 1 WHERE _id = ?`) - but you can't mix query statements (e.g. `SELECT`) which return results and DML statements (e.g. `INSERT`/`UPDATE`/`DELETE`/`ERASE`) in the same transaction.
   
   (Where you need to check invariants before committing a DML transaction, see [`ASSERT`](/reference/main/sql/txs#assert))

2. All transactions that perform writes are serialized via a totally-ordered durable log.

   This means that XT, internally, has very little locking - this gives us excellent per-thread performance, as well as removing a whole category of bugs/errors.
   
   Combined, these two properties provide straightforward [ACID](https://en.wikipedia.org/wiki/ACID) guarantees.

In more detail, XTDB is heavily inspired by the 'Epochal Time Model', as outlined in Clojure and Datomic author Rich Hickey's talk 'The Database as a Value'[^1]:
[^1]: https://www.infoq.com/presentations/Datomic-Database-Value/

![Epochal Time Model](/images/docs/epochal-time-model.webp)

## Transaction consistency

A transaction may be explicitly specified as either `READ ONLY` (a "read-only transaction") or `READ WRITE` (a "DML transaction").
If not specified, this distinction is inferred from the first statement in the transaction after a `BEGIN`.

A DML transaction may contain only DML statements and ASSERT statements.
Transactions are not interactive, and they describe changes atomically.

Within a DML transaction, statements are evaluated sequentially, so later statements see the effects of earlier ones.
Any attempt to (e.g.) SELECT within such a transaction will result in an error.

DML transactions in XTDB are serialized via a totally-ordered log - these are then indexed in order, one at a time, on each of the nodes.
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

## Repeatable queries - 'basis'

Every query statement in XTDB is evaluated at a 'basis' - both a 'snapshot' of the database, which determines the transactions visible to the query, and a 'clock time'.

A read-only transaction (which may only contain query statements), is cheap to create and can be used to easily execute multiple query statements against a consistent basis.

A read-only transaction is not executed as a stateful transaction in the traditional sense.
Instead, it simply defines a long-lived, stable basis context that provides a consistent view of the database at a specific snapshot in time.

Crucially, use of read-only transactions does not require locking or risk degrading performance of concurrent reads and writes.

For more details, see the ['basis' reference documentation](/reference/main/sql/queries#basis).

### Snapshots

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

You may also provide `SNAPSHOT_TIME`, a timestamp which further limits the transactions visible to your query.
If both are provided, transactions will not be visible if they are after either the `SNAPSHOT_TOKEN` or the `SNAPSHOT_TIME`.

```sql
-- to bound all of the queries within a transaction:
BEGIN READ ONLY 
  WITH (SNAPSHOT_TIME = TIMESTAMP '2023-01-01Z');
  
SELECT ...;

ROLLBACK; -- or COMMIT, doesn't matter for read-only transactions in XTDB.

-- alternatively, on a per-query basis:
SETTING SNAPSHOT_TIME = TIMESTAMP '2023-01-01Z'
SELECT ...;
```

:::caution
`SNAPSHOT_TIME` **does not** provide repeatable queries to the same level as `SNAPSHOT_TOKEN` - it is provided only as an approximation for convenience.

For example, if the queried node is behind the `SNAPSHOT_TIME` when you first run a query (i.e. its latest indexed transaction in any database is before that time) and then catches up later, subsequent queries - even with the same `SNAPSHOT_TIME` - may return different results.
Especially in multiple-database systems, the system may never have been in the state suggested by the returned results.

If you need full query repeatability (for audit purposes, say) you should store and re-use the `SNAPSHOT_TOKEN` of your query, as described above - these are guaranteed to reflect the state of the system at the point in time when the token was retrieved.
:::
  
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
