---
title: SQL Queries
---

For examples on how to run SQL queries in each client library, see the [individual driver documentation](/drivers).

## Top-level queries

At the top-level, XTDB SQL queries augment the SQL standard in the following ways:

- `SELECT` is optional - if not provided, it defaults to `SELECT *`
- `FROM` is optional - if not provided, it defaults to a 0-column, 1-row table.

    This enables queries of the form `SELECT 1 + 2` - e.g. to quickly
    evaluate a data-less calculation.

- `SELECT` may optionally be provided between `GROUP BY` and `ORDER BY`, for readability - at the place in the pipeline where it's actually evaluated.
- `GROUP BY` is inferred if not provided to be every column reference used outside of an aggregate function.

    e.g. for `SELECT a, SUM(b) FROM foo`, XT will infer `GROUP BY a`

### query

```railroad
const orderByDirection = rr.Choice(0, rr.Skip(), "ASC", "DESC")
const orderByNulls = rr.Optional(rr.Sequence("NULLS", rr.Choice(0, "FIRST", "LAST")), "skip")
const orderByClause = rr.Optional(rr.Sequence("ORDER", "BY", rr.OneOrMore(rr.Sequence("<value>", orderByDirection, orderByNulls), ",")), "skip")

const limitClause = rr.Sequence("LIMIT", "<limit>")
const offsetClause = rr.Sequence("OFFSET", "<offset>")
const offsetAndLimit = rr.Optional(rr.Choice(0, rr.Sequence(limitClause, rr.Optional(offsetClause, "skip")), rr.Sequence(offsetClause, rr.Optional(limitClause, "skip"))), "skip")
return rr.Diagram(rr.Stack(rr.Sequence(rr.Optional("<with clause>", "skip"), "<query term>"), orderByClause, offsetAndLimit))
```

### with clause

```railroad
const withClause = rr.Sequence(rr.Optional("MATERIALIZED", "skip"), "<query name>", rr.Optional("AS", "skip"), "(", "<query>", ")")
return rr.Diagram(rr.Sequence("WITH", rr.OneOrMore(withClause, ",")))
```

- `WITH` clauses in XTDB are 'optimization fences' - XTDB will not attempt to optimize an outer query into a `WITH` clause, or across `WITH` clauses.
- Supply `MATERIALIZED` to eagerly materialize a `WITH` clause, so that the results can be re-used multiple times in the same query.
- `WITH RECURSIVE` is not yet supported in XTDB.

### query term

```railroad
const selectClause = "<select clause>"
const fromClause = "<from clause>"
const predicates = rr.OneOrMore(rr.Optional('<predicate>', 'skip'), ',')
const whereClause = rr.Sequence("WHERE", predicates)
const groupByClause = rr.Optional(rr.Sequence("GROUP", "BY", rr.OneOrMore("<value>", ",")), "skip")
const havingClause = rr.Optional(rr.Sequence("HAVING", predicates), "skip")
const selectFirst = rr.Sequence(selectClause, rr.Optional(fromClause), rr.Optional(whereClause, "skip"), groupByClause, havingClause)
const fromFirst = rr.Sequence(fromClause, rr.OneOrMore(rr.Choice(0, rr.Skip(), whereClause, rr.Sequence(groupByClause, havingClause, selectClause))))

const colNames = rr.Sequence("(", rr.OneOrMore("<column name>", ","), ")")
const doc = rr.Sequence("(", rr.OneOrMore("<value>", ","), ")")
const docs = rr.OneOrMore(doc, ",")
const values = rr.Sequence("VALUES", docs)
const xtql = rr.Sequence("XTQL", rr.Choice(0, "<XTQL query>", rr.Sequence('(', '<XTQL query>', rr.ZeroOrMore(rr.Sequence(',', '?'), null, 'skip'), ')')))

const allOrDistinct = rr.Choice(0, rr.Skip(), "ALL", "DISTINCT")
const binary = rr.Sequence("<query term>", rr.Choice(0, "UNION", "INTERSECT", "EXCEPT"), allOrDistinct, "<query term>")
return rr.Diagram(rr.Choice(0, selectFirst, fromFirst, values, xtql, binary, rr.Sequence("(", "<query term>", ")")));
```

NB:

- `SELECT` is optional in XTDB - if not provided, it defaults to `SELECT *`
- `SELECT` may be placed after `GROUP BY` in XTDB, so that the query clauses are written in the order that they're executed in practice.
- `GROUP BY` is optional in XTDB - if not provided, it defaults to all of the columns used outside of an aggregate function.
- If you start your query with `FROM`, you may then include arbitrarily many sets of `WHERE`/`GROUP BY`/`SELECT` clauses, which will be evaluated in order.
- Predicates can be comma-separated in XTDB, to aid with SQL generation - these are treated as conjuncts.
  There may be an arbitrary number of commas at the start, between any two predicate expressions, or at the end.
- XTQL queries are sent within an SQL string literal - e.g.
`'(-> (from ...) ...)'`.
Given the possible presence of single quotes within the query, it is recommended to use dollar-delimited strings here: `XTQL $$ <xtql query> $$`

Due to implementation details in some drivers (e.g. PGJDBC), it is required to additionally specify the params in standard SQL (`?`) following your XTQL query, so that the driver knows how many arguments to allow.

For more details on XTQL queries, see the [XTQL documentation](/xtql/tutorials/introducing-xtql).

### select clause

```railroad
const starOpts = rr.Sequence(rr.Optional('<star exclude>', 'skip'), rr.Optional('<star rename>', 'skip'))
const selectCol = rr.Sequence("<value>", rr.Optional(rr.Sequence(rr.Optional("AS", "skip"), "<column name>"), 'skip'))
const qualifiedStar = rr.Sequence('<table name>', '.', '*', starOpts)
return rr.Diagram(rr.Sequence("SELECT", rr.Stack(rr.Optional(rr.Sequence('*', starOpts), 'skip'), rr.OneOrMore(rr.Choice(0, rr.Skip(), selectCol, qualifiedStar), ","))))
```

#### star exclude

```railroad
return rr.Diagram(rr.Sequence('EXCLUDE', rr.Choice(0, '<column name>', rr.Sequence('(', rr.OneOrMore('<column name>', ','), ')'))))
```

#### star rename

```railroad
const renameCol = rr.Sequence('<column name>', 'AS', '<column name>')
return rr.Diagram('RENAME', rr.Choice(0, renameCol, rr.Sequence('(', rr.OneOrMore(renameCol, ','), ')')))
```

## From clause, joins

### from clause

```railroad
const tableProjection = rr.Optional(rr.Sequence("(", rr.OneOrMore("<column name>", ","), ")"), "skip")
return rr.Diagram("FROM", rr.OneOrMore(rr.Sequence("<relation>", tableProjection), ","))
```

### relation

```railroad
const wrapped = rr.Sequence("(", "<relation>", ")")
const tableAlias = "<table alias>"
const base = rr.Sequence(rr.Choice(0, "<table name>", "<query name>"), rr.ZeroOrMore("<temporal filter>", null, "skip"), rr.Optional(tableAlias, "skip"))

const joinType = rr.Choice(0, rr.Skip(), rr.Sequence(rr.Choice(0, "LEFT", "RIGHT"), rr.Optional("OUTER", "skip")), "INNER")
const onClause = rr.Sequence("ON", "<predicate>")
const usingClause = rr.Sequence("USING", "(", rr.OneOrMore("<column name>", ","), ")")
const join = rr.Sequence("<relation>", joinType, "JOIN", "<relation>", rr.Choice(0, onClause, usingClause))

const crossJoin = rr.Sequence("<relation>", "CROSS", "JOIN", "<relation>")
const naturalJoin = rr.Sequence("<relation>", "NATURAL", joinType, "JOIN", "<relation>")

const colNames = rr.Sequence("(", rr.OneOrMore("<column name>", ","), ")")
const doc = rr.Sequence("(", rr.OneOrMore("<value>", ","), ")")
const docs = rr.OneOrMore(doc, ",")
const values = rr.Sequence("VALUES", docs)

const xtql = rr.Sequence("XTQL", rr.Choice(0, "<XTQL query>", rr.Sequence('(', '<XTQL query>', rr.ZeroOrMore(rr.Sequence(',', '?'), null, 'skip'), ')')))
const subquery = rr.Sequence("(", "<query>", ")")
const lateral = rr.Sequence("LATERAL", subquery)
const unnest = rr.Sequence("UNNEST", "(", "<value>", ")", rr.Optional(rr.Sequence("WITH", "ORDINALITY"), "skip"))
const subqs = rr.Sequence(rr.Choice(0, values, xtql, subquery, lateral, unnest), tableAlias)

return rr.Diagram(rr.Choice(0, wrapped, base, join, crossJoin, naturalJoin, subqs))
```

- See [query term](#query-term) for details on XTQL queries.

### temporal filter

```railroad
const asOf = rr.Sequence("AS", "OF", "<timestamp>")
const fromTo = rr.Sequence("FROM", "<timestamp>", "TO", "<timestamp>")
const between = rr.Sequence("BETWEEN", "<timestamp>", "AND", "<timestamp>")
const timePeriodOpts = rr.Choice(0, asOf, fromTo, between, "ALL")
const timePeriod = rr.Choice(0, "VALID_TIME", "SYSTEM_TIME")
return rr.Diagram(rr.Sequence("FOR", rr.Choice(0, rr.Sequence(timePeriod, timePeriodOpts), rr.Sequence("ALL", timePeriod))))
```

The valid-time/system-time filters for a given table take precedence as follows:

1. Any explicit specifications in the `FROM` clause.
2. Any options passed to the `SETTING` clause of the given query (see ['Basis'](#basis)).
3. Any options passed to the `BEGIN` clause of the current transaction (see ['Basis'](#basis)).
4. Then as follows:
   * System time defaults to 'as best known' - the latest processed transaction on the queried node.
   * Valid time defaults to 'as of now' - the clock time taken either from `CLOCK_TIME` (if overridden) or the actual clock time on the queried node.

## Expressions

### value

```railroad
const colRef = rr.Choice(0, "<column reference>", rr.Sequence('"', "<column reference>", '"'), rr.Sequence('`', "<column reference>", '`'))
const unaryOp = rr.Sequence(rr.HorizontalChoice("+", "-"), "<value>")
const binaryOp = rr.Sequence("<value>", rr.HorizontalChoice("+", "-", "*", "/"), "<value>")

const fn = rr.Sequence("<function name>", "(", rr.ZeroOrMore("<value>", ",", "skip"), ")")

const rowNumber = rr.Sequence("ROW_NUMBER", "(", ")")
const leadLagOffset = rr.Optional(rr.Sequence(",", "<offset>"), "skip")
const leadLag = rr.Sequence(rr.Choice(0, "LEAD", "LAG"), "(", "<value>", leadLagOffset, ")")
const wfn = rr.Sequence(rr.Choice(0, rowNumber, leadLag), "OVER", "(", "<window>", ")")

const cast = rr.Sequence("CAST", "(", "<value>", "AS", "<data type>", ")")
const caseValue = rr.Sequence("<value>", rr.ZeroOrMore(rr.Sequence("WHEN", "<value>", "THEN", "<value>"), ","))
const casePreds = rr.ZeroOrMore(rr.Sequence("WHEN", "<predicate>", "THEN", "<value>"), ",")
const caseExpr = rr.Sequence("CASE", rr.Choice(0, caseValue, casePreds), rr.Optional(rr.Sequence("ELSE", "<value>"), "skip"), "END")

const coalesce = rr.Sequence("COALESCE", "(", rr.ZeroOrMore("<value>", ","), ")")
const nullIf = rr.Sequence("NULLIF", "(", "<value>", ",", "<value>", ")")

const arrayLiteral = rr.Sequence(rr.Optional("ARRAY", "skip"), "[", rr.ZeroOrMore("<value>", ",", "skip"), "]")
const arrayByQuery = rr.Sequence("ARRAY", "(", "<query>", ")")
const arrayConstructor = rr.Choice(0, arrayLiteral, arrayByQuery)

const wrapped = rr.Sequence("(", "<value>", ")")

const subqs = rr.Sequence(rr.Choice(0, rr.Skip(), "NEST_ONE", "NEST_MANY"), "(", "<query>", ")")

return rr.Diagram(rr.Choice(0, "<literal>", colRef, "<param>", unaryOp, binaryOp, fn, wfn, "<predicate>", cast, caseExpr, coalesce, nullIf, arrayConstructor, "<record>", subqs, wrapped))
```

### param

```railroad
return rr.Diagram(rr.Choice(0, "?", "$<param idx>"))
```

### record

```railroad
const objectEntries = rr.ZeroOrMore(rr.Sequence("<field name>", ":", "<value>"), ",", "skip")
const objectBraceConstructor = rr.Sequence("{", objectEntries, "}")
const objectFnConstructor = rr.Sequence(rr.Choice(0, "RECORD", "OBJECT"), "(", objectEntries, ")")

return rr.Diagram(rr.Choice(0, objectBraceConstructor, objectFnConstructor))
```

### literal

```railroad
const stringLiteral = rr.Choice(0, rr.Sequence("'", "<SQL-style string>", "'"), rr.Sequence('$$', '<string>', '$$'), rr.Sequence("E'", "<C-style string>", "'"))

const dateLiteral = rr.Sequence("DATE", "'", "<ISO8601 date literal>", "'")
const timeLiteral = rr.Sequence("TIME", "'", "<ISO8601 time literal>", "'")
const isoTimestampLiteral = rr.Sequence("'", "<ISO8601 timestamp literal>", "'")
const sqlTimestampLiteral = rr.Sequence(rr.Choice(0, rr.Skip(), rr.Sequence(rr.Choice(0, "WITH", "WITHOUT"), "TIME", "ZONE")), "'", "<SQL timestamp literal>", "'")
const timestampLiteral = rr.Sequence("TIMESTAMP", rr.Choice(0, isoTimestampLiteral, sqlTimestampLiteral))
const durationLiteral = rr.Sequence("DURATION", "'", "<ISO8601 duration literal>", "'")
const dateTimeLiteral = rr.Choice(0, dateLiteral, timeLiteral, timestampLiteral, durationLiteral)

return rr.Diagram(rr.Choice(0, "NULL", "<numeric literal>", stringLiteral, dateTimeLiteral))
```

- See [Date/time types](/reference/main/data-types.html#datetime-types) for more details on XTDB's timestamp literals.

### predicate

```railroad
const maybeNot = rr.Optional("NOT", "skip")

const booleanLiteral = rr.HorizontalChoice("TRUE", "FALSE")
const unaryNot = rr.Sequence("NOT", "<predicate>")
const binaryPred = rr.Sequence("<predicate>", rr.HorizontalChoice("AND", "OR"), "<predicate>")
const binaryFn = rr.Sequence("<value>", rr.HorizontalChoice("=", "<>", "!=", "<", "<=", ">=", ">"), rr.Choice(0, "<value>", rr.Sequence(rr.Choice(0, "ANY", "ALL"), "(", "<query>", ")")))
const predFn = rr.Sequence("<predicate name>", "(", rr.ZeroOrMore("<value>", ",", "skip"), ")")
const isPredicate = rr.Sequence("<value>", "IS", rr.Optional("NOT", "skip"), rr.HorizontalChoice("TRUE", "FALSE", "NULL"))
const exists = rr.Sequence(maybeNot, "EXISTS", "(", "<query>", ")")
const inPredicate = rr.Sequence("<value>", maybeNot, "IN", rr.Sequence("(", rr.Choice(1, rr.Skip(), rr.OneOrMore("<value>", ","), "<query>"), ")"))
const likePredicate = rr.Sequence("<value>", maybeNot, "LIKE", "<value>", rr.Optional(rr.Sequence("ESCAPE", "'", "<escape character>", "'"), "skip"))
const likeRegexPredicate = rr.Sequence("<value>", maybeNot, "LIKE_REGEX", "<JVM regex>", rr.Optional(rr.Sequence("FLAG", "'", "<JVM regex flags>", "'"), "skip"))
const postgresRegexPredicate = rr.Sequence("<value>", rr.HorizontalChoice("~", "~*", "!~", "!~*"), "<JVM regex>")
const betweenPredicate = rr.Sequence("<value>", maybeNot, "BETWEEN", rr.Choice(0, rr.Skip(), "ASYMMETRIC", "SYMMETRIC"), "<value>", "AND", "<value>")
return rr.Diagram(rr.Choice(0, booleanLiteral, unaryNot, binaryPred, binaryFn, predFn, isPredicate, exists, inPredicate, likePredicate, likeRegexPredicate, postgresRegexPredicate, betweenPredicate))
```

### window

```railroad
const wfnPartition = rr.Sequence("PARTITION", "BY", rr.OneOrMore("<value>", ","))
const wfnOrderDirection = rr.Choice(0, rr.Skip(), "ASC", "DESC")
const wfnOrderNulls = rr.Optional(rr.Sequence("NULLS", rr.Choice(0, "FIRST", "LAST")), "skip")
const wfnOrder = rr.Sequence("ORDER", "BY", rr.OneOrMore(rr.Sequence("<value>", wfnOrderDirection, wfnOrderNulls), ","))

return rr.Diagram(rr.Optional(wfnPartition), rr.Optional(wfnOrder))
```

Note:

- `LEAD`/`LAG` currently only support column references, not arbitrary expressions.
- The default value parameter to `LEAD`/`LAG` is not yet supported.
- `IGNORE NULLS` with `LEAD`/`LAG` is not yet supported.

## Nested sub-queries

Nested sub-queries allow you to easily create tree-shaped results, using `NEST_MANY` and `NEST_ONE`:

- For example, if you have a one-to-many relationship (e.g. customers → orders), you can write a query that, for each customer, returns an array of their orders as nested objects:

    ``` sql
    SELECT c._id AS customer_id, c.name,
           NEST_MANY(SELECT o._id AS order_id, o.value
                     FROM orders o
                     WHERE o.customer_id = c._id
                     ORDER BY o._id)
             AS orders
    FROM customers c
    ```

    ⇒

    ``` json
    [
      {
        "customerId": 0,
        "name": "bob",
        "orders": [ { "orderId": 0, "value": 26.20 }, { "orderId": 1, "value": 8.99 } ]
      },
      {
        "customerId": 1,
        "name": "alice",
        "orders": [ { "orderId": 2, "value": 12.34 } ]
      }
    ]
    ```

- In the other direction (many-to-one) - for each order, additionally return details about the customer - use `NEST_ONE` to get a single nested object:

  ``` sql
  SELECT o._id AS order_id, o.value,
         NEST_ONE(SELECT c.name FROM customers c
                  WHERE c._id = o.customer_id)
           AS customer
  FROM orders o
  ORDER BY o._id
  ```

  ⇒

  ``` json
  [
    {
      "orderId": 0,
      "value": 26.20,
      "customer": { "name": "bob" }
    },
    {
      "order-id": 1,
      "value": 8.99,
      "customer": { "name": "bob" }
    },
    {
      "order-id": 2,
      "value": 12.34,
      "customer": { "name": "alice" }
    }
  ]
  ```

## Basis

Queries in XTDB run against a 'basis', which consists of:

1. a 'snapshot' - an upper bound on the transactions that are visible to the query.
2. a 'clock time' - used for any function calls that reference the current time (e.g. `CURRENT_TIMESTAMP`)

These can be set either on a per-query basis, using `SETTING`, or at the start of a transaction, using `BEGIN`:

### SETTING

```railroad
const asOf = rr.Sequence("AS", "OF", "<timestamp>")
const fromTo = rr.Sequence("FROM", "<timestamp>", "TO", "<timestamp>")
const between = rr.Sequence("BETWEEN", "<timestamp>", "AND", "<timestamp>")
const timePeriodOpts = rr.Choice(0, asOf, fromTo, between, "ALL")

const defaultTimePeriods = rr.Sequence("DEFAULT", rr.Choice(0, "VALID_TIME", "SYSTEM_TIME"), rr.Optional("TO", "skip"), timePeriodOpts)
const basis = rr.Sequence(rr.Choice(0, "SNAPSHOT_TOKEN", "CLOCK_TIME"), rr.Choice(0, "TO", "="), "<timestamp>")
const setting = rr.Sequence("SETTING", rr.OneOrMore(rr.Choice(0, defaultTimePeriods, basis), ","))
return rr.Diagram(rr.Sequence(rr.Optional(setting, "skip"), "<query>"))
```

* Setting the default valid-time/system-time applies to any `FROM` clause that doesn't have any valid-time/system-time specification explicitly set.
* Setting the `SNAPSHOT_TOKEN` enforces an upper-bound on the transactions visible to the query - i.e. no matter what the per-table system-time clauses specify, they will not see anything newer than this snapshot-token.
  If not provided, this defaults to the latest-completed transaction on the queried node.
* Setting the `CLOCK_TIME` defines a fixed value for any functions that depend on the current time - e.g. `CURRENT_TIMESTAMP`.
  It also defines the default valid-time selection for any tables in `FROM` clauses that don't otherwise have a valid-time specification.
  If not provided, it defaults to the clock-time fixed at the start of the transaction.
  
### BEGIN / COMMIT / ROLLBACK

```railroad
const eq = rr.Optional('=')

const tz = rr.Sequence(rr.Choice(0, 'TIMEZONE', rr.Sequence('TIME', 'ZONE')), eq, '<timezone>')

const roOpts = rr.Choice(0, 
  rr.Skip(),
  rr.Sequence('SNAPSHOT_TOKEN', eq, '<snapshot token>'), 
  rr.Sequence('CLOCK_TIME', eq, '<timestamp>'),
  rr.Sequence('AWAIT_TOKEN', eq, '<await token>'),
  tz
)

const ro = rr.Sequence("READ", "ONLY", rr.Optional(rr.Sequence("WITH", "(", rr.OneOrMore(roOpts, ","), ")"), "skip"))

const rw = rr.Sequence("READ", "WRITE", '...')

const begin = rr.Sequence('BEGIN', rr.Optional(rr.Choice(0, ro, rw), 'skip'))

return rr.Diagram(rr.Choice(0, begin, 'COMMIT', 'ROLLBACK'))
```

* A transaction may be either `READ ONLY` or `READ WRITE`.
  If not specified, it will be inferred from the first statement in the transaction.

  Transactions must not mix query statements and [DML](https://en.wikipedia.org/wiki/Data_manipulation_language) statements.
* Additionally, for read-only transactions:
  * `SNAPSHOT_TOKEN` and `CLOCK_TIME` behave the same as in [`SETTING`](#setting).
  * `AWAIT_TOKEN` may be provided to wait for a specific transaction to be visible on the queried node before starting the transaction.
    If not provided, it defaults to waiting for the latest-submitted transaction on the current connection.
  * `TIMEZONE` sets the time zone for the duration of the transaction, affecting any time zone-aware date/time literals and functions.
    If not provided, it defaults to the time-zone of the connection.
* For read-write transactions, see the [transaction reference](/reference/main/sql/txs#begin--commit--rollback).
* Committing/rolling back a read-only transaction has no effect in XTDB, because readers never block writers nor each other.
