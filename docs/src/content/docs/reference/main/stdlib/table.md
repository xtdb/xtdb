---
title: Table functions
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: `GENERATE_SERIES` is end-inclusive

: `GENERATE_SERIES(start, end)` now includes `end` in its output, matching PostgreSQL and DuckDB.

  Previously, `GENERATE_SERIES` was end-exclusive — equivalent to the new `RANGE` function.

  Upgrade steps:

  - Either: migrate queries from `GENERATE_SERIES` to `RANGE` to keep end-exclusive behaviour.
  - Or: leave queries as-is and accept the new inclusive semantics — most callers will want this.
  - Escape hatch: set `XTDB_GENERATE_SERIES_END_EXCLUSIVE=true` on the node to revert `GENERATE_SERIES` to its pre-2.2 (end-exclusive) behaviour while you migrate.
    `RANGE` is unaffected by this flag.

</details>

Functions usable in the `FROM` clause of a query to produce a relation.

See [`table reference`](/reference/main/sql/queries#table-reference) in the query grammar for how these slot into the wider `FROM` clause alongside `VALUES`, `UNNEST`, sub-queries, and so on.

## Series-generating functions

`RANGE(start, end [, stride])` (v2.2+)
: generates a series of values from `start` (inclusive) to `end` (**exclusive**), with the given `stride`.

  - Works over integers (stride defaults to `1`) and temporal types (stride is an `INTERVAL`).
  - Time-zone-aware: if `start`/`end` carry a time-zone, the series honours any daylight-savings transitions between them — note the difference between adding `INTERVAL 'P1D'` (1 calendar day) and `INTERVAL 'PT24H'` (24 hours) across a DST boundary.
  - XTDB's time-zone handling is an extension to PostgreSQL's `generate_series`.

  ```sql
  FROM RANGE(DATE '2020-01-01', DATE '2020-01-04', INTERVAL '1' DAY)
  -- yields: [DATE '2020-01-01', DATE '2020-01-02', DATE '2020-01-03']

  FROM RANGE(TIMESTAMP '2020-01-01T00:00:00Z',
             TIMESTAMP '2020-01-01T01:00:00Z',
             INTERVAL 'PT15M')
  -- yields: [00:00Z, 00:15Z, 00:30Z, 00:45Z]

  FROM RANGE(TIMESTAMP '2020-03-29T00:00:00Z[Europe/London]',
             TIMESTAMP '2020-03-31T00:00:00+01:00[Europe/London]',
             INTERVAL 'P1D')
  -- yields: [2020-03-29T00:00:00Z[Europe/London], 2020-03-30T00:00:00+01:00[Europe/London]]
  ```

`GENERATE_SERIES(start, end [, stride])`
: like `RANGE`, but with an **inclusive** end bound — matching PostgreSQL's `generate_series` semantics.

  ```sql
  FROM GENERATE_SERIES(1, 4)
  -- yields: [1, 2, 3, 4]

  FROM GENERATE_SERIES(DATE '2020-01-01', DATE '2020-01-04', INTERVAL '1' DAY)
  -- yields: [DATE '2020-01-01', DATE '2020-01-02', DATE '2020-01-03', DATE '2020-01-04']
  ```

## Scalar functions in FROM

Any scalar function call (v2.2+) may be used as a table source, in which case its result is wrapped as a single-row table whose default column name is the function name.

- `WITH ORDINALITY` is supported.
- Aggregate functions are rejected with a clear error.

```sql
FROM STRING_TO_ARRAY('a,b,c', ',') AS parts
-- single-row table with column `parts` = ['a', 'b', 'c']
```

## Ordinality

Any table function may be suffixed with `WITH ORDINALITY` to append a 1-based row-number column to the output:

```sql
FROM GENERATE_SERIES(1, 4) WITH ORDINALITY AS t(n, idx)
-- n | idx
-- --+----
-- 1 |  1
-- 2 |  2
-- 3 |  3
```
