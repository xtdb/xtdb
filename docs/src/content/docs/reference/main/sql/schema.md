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
