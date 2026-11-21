---
title: SQL Schema
---

XTDB's schema is largely inferred, but you may create tables ahead-of-time if required, to prevent table-not-found or column-not-found errors.

Normally this isn't required - you can simply `INSERT` data to get started.

## CREATE TABLE (v2.2+)

Tables can be created or altered with the `CREATE TABLE` statement.

```railroad
const createTable = rr.Sequence("CREATE", rr.Optional(rr.Sequence("OR", "ALTER"), "skip"), "TABLE", "<table name>")
const colNames = rr.Sequence("(", rr.OneOrMore("<column name>", ","), ")")
return rr.Diagram(rr.Sequence(createTable, rr.Optional(colNames, "skip")))
```

* Column names are optional and do not yet support a type declaration.
* `CREATE TABLE` does not fail if the table exists; any columns declared will be added to the existing table.

e.g.:

```sql
CREATE TABLE users (_id, name, email);
```

## CREATE INDEX (v2.3+)

A secondary index on a column lets a query that compares that column for equality read only the pages holding the rows that could match, rather than every page of the table.

```railroad
return rr.Diagram(rr.Sequence("CREATE", "INDEX", "<index name>", "ON", "<table name>", "(", "<column name>", ")"))
```

e.g.:

```sql
CREATE INDEX users_by_email ON users (email);

SELECT * FROM users WHERE email = 'ada@example.com';
SELECT * FROM users WHERE email = ?;
```

* An index belongs to the database the statement runs against, and covers one column of one table.
* Index names are unique within a database: declaring a name that's already declared fails with 'Index already exists'.
* The table and column needn't exist yet — they come into existence with their first write, and the index covers them from then on.
* `CREATE INDEX` runs on its own, outside a transaction: within `BEGIN` … `COMMIT`, it fails.
* External-source databases don't accept `CREATE INDEX`.

On a table that already holds data, XTDB builds the index in the background; until it's built, queries read the table as they would without it.
Once it's built, every query that can use it does — you don't need to change your queries.

A query uses an index when it compares the indexed column with `=` to a literal or a parameter that is a number, a string, a boolean or a UUID.
Any other predicate — a range, a `LIKE`, a comparison with another column, a date or timestamp value — and any join, reads the table as it would without the index.
An index never changes which rows a query returns.

`EXPLAIN ANALYZE` shows the effect: a scan that uses an index counts the index's own files and pages in `scan_files_used` and `scan_pages_used`, alongside the data it reads.

## DROP INDEX (v2.3+)

```railroad
return rr.Diagram(rr.Sequence("DROP", "INDEX", "<index name>"))
```

e.g.:

```sql
DROP INDEX users_by_email;
```

* Queries from the drop onwards read the table without the index, and the name is free to declare again.
* Dropping a name that isn't declared fails with 'Index does not exist'.
* Like `CREATE INDEX`, `DROP INDEX` runs outside a transaction.
