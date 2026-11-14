---
title: Aggregate functions
---

<details>
<summary>Changelog (last updated v2.2.1)</summary>

v2.2.1: percentiles spill to disk, and reject an out-of-range fraction

: [`PERCENTILE_CONT` and `PERCENTILE_DISC`](#ordered-set-aggregate-functions) spill their input to disk past an internal threshold, and reject a fraction outside `[0, 1]`.

  Previously both held every input row in memory, so a large enough input could exhaust the heap.
  An out-of-range fraction's outcome depended on the data: PERCENTILE_CONT threw an internal error on groups of two or more rows but returned a value on a one-row group, and PERCENTILE_DISC returned the first or last value.

  Upgrade step: a query that passes a fraction outside `[0, 1]` needs one within it.

</details>

Aggregate functions can be used within `SELECT` clause.

In line with the SQL spec:

- Null values in the column are removed before the aggregate is calculated, except in `COUNT(*)`, which counts rows, and `ARRAY_AGG`, which keeps them.
- Without grouping columns, aggregate functions will always return exactly one row - if the input column is empty (after nulls have been removed), the result will be a single row containing a null value.

## `FILTER` (v2.2+)

Any aggregate function may be followed by `FILTER (WHERE <condition>)`, restricting that aggregate to the rows where the condition is true.
Rows where the condition is false or null are excluded.

Filtered and unfiltered aggregates can appear alongside each other over the same groups:

```sql
SELECT dept,
       COUNT(*) AS headcount,
       COUNT(*) FILTER (WHERE status = 'active') AS active,
       SUM(salary) FILTER (WHERE status = 'active') AS active_payroll
FROM employees
GROUP BY dept
```

The condition may reference any column of the input, whether or not it is selected or grouped.
It may not contain another aggregate function.

For a group where no row passes the filter, `COUNT` returns 0 and every other aggregate returns null — the same answers those functions give over an empty input.

## Numeric aggregate functions

- `AVG([ALL] xs)` (average (mean) of all values)
- `AVG(DISTINCT xs)` (average (mean) of distinct values)
- `COUNT([ALL] xs)` (count of rows that contain non-null values)
- `COUNT(DISTINCT xs)` (count of distinct values)
- `COUNT(*)` (row count)
- `MAX([ALL|DISTINCT] xs)` (maximum value)
- `MIN([ALL|DISTINCT] xs)` (minimum value)
- `STDDEV_POP(xs)` (population standard deviation)
- `STDDEV_SAMP(xs)` (sample standard deviation)
- `SUM([ALL] xs)` (sum of values)
- `SUM(DISTINCT xs)` (sum of distinct values)
- `VAR_POP(xs)` (population variance)
- `VAR_SAMP(xs)` (sample variance)

## Boolean aggregate functions

- `BOOL_AND(xs)` / `EVERY(xs)` (true if all values are true; false otherwise)
- `BOOL_OR(xs)` (false if all values are false; true otherwise)

Note: In keeping with Postgres, we rename `ALL` and `ANY` to `BOOL_AND` and `BOOL_OR` to avoid confusion with the logical operators. `EVERY` is a SQL-standard alias for `BOOL_AND`.

## Composite-type aggregate functions

- `ARRAY_AGG(xs)` (return an array of all of the input values)
- `ARRAY_AGG(xs ORDER BY ys [ASC|DESC] [NULLS FIRST|LAST], ...)` (v2.2+) (as above, with the values in the given order)

## Ordered-set aggregate functions

`PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY col)` (v2.2+)
: continuous percentile — the value at position `fraction` (in `[0, 1]`) along the sorted values, interpolating between adjacent values if necessary.

  - `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col)` gives the median.
  - `col` must be numeric.
  - PostgreSQL-compatible.

  ```sql
  SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median_amount
  FROM sales
  ```

`PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY col)` (v2.2+)
: discrete percentile — the first of the sorted values whose cumulative distribution reaches `fraction` (in `[0, 1]`); always one of the input values, never interpolated.

  - `col` can be of any orderable type — strings and timestamps as well as numbers.
  - PostgreSQL-compatible.

  ```sql
  SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY region) AS middle_region
  FROM sales
  ```

Both functions accept `ORDER BY col DESC`, which measures `fraction` from the largest value down.
`NULLS FIRST` and `NULLS LAST` are accepted but have no effect, since nulls are excluded before ranking.

A `fraction` outside `[0, 1]`, NaN included, is an error.

Past an internal threshold, both functions spill their input to disk under the JVM's temporary directory (`java.io.tmpdir`), as ORDER BY and joins do, rather than hold it in memory, so a query needs temp space in proportion to its input.
