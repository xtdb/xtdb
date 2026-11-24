---
title: SQL Schema
---

<details>
<summary>Changelog (last updated v2.2.1)</summary>

v2.2.1: `CREATE TABLE` declares a table's primary key

: A table can be keyed on a column other than `_id`, or on several columns, declared with `PRIMARY KEY` before anything is written to the table — see [Primary keys](#primary-keys-v221).

  Previously every table was keyed on `_id`, and a table that declares no key still is.
  In a table keyed on another column, `_id` is an ordinary optional column.

  `PRIMARY` and `KEY` are keywords, so `quote_ident('primary')` and `quote_ident('key')` quote them, as `quote_ident` quotes XTDB's other keywords.

  Declare a primary key only once every node in the cluster runs this version: an earlier node ignores the declaration, keys the table's writes on `_id`, and - if it leads - writes blocks that drop the declaration for good.

</details>

XTDB's schema is largely inferred, but you may create tables ahead-of-time if required, to prevent table-not-found or column-not-found errors.

Normally this isn't required - you can simply `INSERT` data to get started.

## CREATE TABLE (v2.2+)

Tables can be created or altered with the `CREATE TABLE` statement.

```railroad
const createTable = rr.Sequence("CREATE", rr.Optional(rr.Sequence("OR", "ALTER"), "skip"), "TABLE", "<table name>")
const column = rr.Sequence("<column name>", rr.Optional(rr.Sequence("PRIMARY", "KEY"), "skip"))
const tablePk = rr.Sequence("PRIMARY", "KEY", "(", rr.OneOrMore("<column name>", ","), ")")
const elements = rr.Sequence("(", rr.OneOrMore(rr.Choice(0, column, tablePk), ","), ")")
return rr.Diagram(rr.Sequence(createTable, rr.Optional(elements, "skip")))
```

* Column names are optional and do not yet support a type declaration.
* `CREATE TABLE` does not fail if the table exists; any columns declared will be added to the existing table.

e.g.:

```sql
CREATE TABLE users (_id, name, email);
```

### Primary keys (v2.2.1+)

A table's primary key is the column, or columns, whose values identify a row: writes with the same key values are versions of the same record, and an `INSERT` with key values that are already there upserts it.
Unless a table declares otherwise, its key is `_id`.

Declare a different key either on the column or as a table element:

```sql
CREATE TABLE users (email PRIMARY KEY, name);
CREATE TABLE orders (order_no, customer, PRIMARY KEY (order_no));
```

Every write into the table then supplies the key column rather than `_id`:

```sql
INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice');

-- an upsert: same key, so a new version of the same record
INSERT INTO users (email, name) VALUES ('alice@example.com', 'Alice Smith');

SELECT name FROM users WHERE email = 'alice@example.com';
```

An equality on a single-column key is looked up directly, as one on `_id` is for a table keyed on `_id`.

* A table has at most one `PRIMARY KEY` declaration, and it names each of its columns once.
  A system column (`_iid`, `_valid_from`, `_valid_to`, `_system_from`, `_system_to`) can't be part of a key.
* The key can only be declared or changed before anything is written to the table.
  Once anything has been written to it - a put, a delete or an erase, including one before any declaration, which keys the table on `_id` - a `CREATE TABLE` declaring a different key aborts its transaction with `xtdb/primary-key-on-non-empty-table`.
  Re-declaring the key the table already has is allowed.
* A declaration applies to the writes after it in the same transaction.
* In a table keyed on another column, `_id` is an ordinary column: a row needn't have one, it can hold a value of any type, and `UPDATE` can set it.
* `SELECT *` lists the key columns first.

#### Composite keys

A key declared over several columns identifies a row by all of their values together, in the order the declaration lists them:

```sql
CREATE TABLE order_lines (order_no, line_no, product, qty, PRIMARY KEY (order_no, line_no));

INSERT INTO order_lines (order_no, line_no, product, qty) VALUES (1001, 1, 'widget', 3), (1001, 2, 'gadget', 1);

-- an upsert: the same order_no and line_no
INSERT INTO order_lines (order_no, line_no, product, qty) VALUES (1001, 2, 'gadget', 2);
```

* Every write supplies every key column: an `INSERT`, `put-docs` or `patch-docs` missing one is an `xtdb.tx/missing-id` error, or a planning error for an `INSERT ... SELECT`.
* `UPDATE` can't set any key column.
* Each key column's value is one of the [ID types](/reference/main/sql/txs#id-types).
