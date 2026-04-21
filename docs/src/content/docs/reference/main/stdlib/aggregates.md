---
title: Aggregate functions
---

Aggregate functions can be used within `SELECT` clause.

In line with the SQL spec:

- Except in the case of `COUNT(*)`, null values in the column are removed before the aggregate is calculated.
- Without grouping columns, aggregate functions will always return exactly one row - if the input column is empty (after nulls have been removed), the result will be a single row containing a null value.

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
