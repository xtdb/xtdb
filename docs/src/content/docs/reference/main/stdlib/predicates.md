---
title: Predicates
---

:::note
- These apply to any data types that are naturally comparable: numbers, strings, date-times, durations, etc.
- XTDB predicates all use [three-valued logic](https://en.wikipedia.org/wiki/Three-valued_logic), as per the SQL spec - unless otherwise specified, if any input expression is null, the result will also be null.
:::

The standard comparators are available:

- `expr1 < expr2` (less than)
- `expr1 <= expr2` (less than or equal to)
- `expr1 > expr2` (greater than)
- `expr1 >= expr2` (greater than or equal to)
- `expr1 = expr2` (equal to)
- `expr1 <> expr2` | `expr1 != expr2` (not equal to)

## Greatest / Least

These aren't strictly predicates - they return the greatest/least value of their arguments respectively:

`GREATEST(expr, ...)`
: returns the greatest value of the provided arguments, by the usual comparison operators

`LEAST(expr, ...)`
: returns the least value of the provided arguments, by the usual comparison operators

## Boolean functions

`expr1 AND expr2`
: returns true if both `expr1` and `expr2` are true, false otherwise

`expr1 OR expr2`
: returns true if either `expr1` or `expr2` are true, false otherwise

`NOT expr`
: returns true if `expr` is false, false otherwise

`expr IS [NOT] TRUE`
: returns true if `expr` is \[not\] true, false otherwise (including if `expr` is null)

`expr IS [NOT] FALSE`
: returns true if `expr` is \[not\] false, false otherwise (including if `expr` is null)

`expr IS [NOT] NULL`
: returns true if `expr` is \[not\] null, false otherwise
