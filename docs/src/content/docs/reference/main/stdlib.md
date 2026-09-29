---
title: Standard Library
---

<details>
<summary>Changelog (last updated v2.3)</summary>

v2.3: SQL operator precedence follows PostgreSQL

: Operators bind as in the [operator precedence](#operator-precedence) table below.

  Previously comparisons bound tighter than `LIKE`, `IN`, the regex operators and the period predicates, `IS TRUE`/`IS FALSE` tighter than comparisons, `||` tighter than arithmetic, and `&` tighter than `|`, `#` and the shifts.
  So `a = b LIKE c` read as `(a = b) LIKE c`, `'total: ' || x + 1` as `('total: ' || x) + 1`, and `5 | 3 & 1` as `5 | (3 & 1)`.
  These now read as they do in PostgreSQL.

  Queries relying on the previous binding need parentheses to keep their meaning; there are no other upgrade steps.

</details>

XTDB provides a rich standard library of predicates and functions:

- [Predicates](/reference/main/stdlib/predicates)
- [Numeric functions](/reference/main/stdlib/numeric)
- [String functions](/reference/main/stdlib/string)
- [Temporal functions](/reference/main/stdlib/temporal)
- [Aggregate functions](/reference/main/stdlib/aggregates)
- [Table functions](/reference/main/stdlib/table)
- [Other functions](/reference/main/stdlib/other)

## Operator precedence

SQL operators bind tightest first, as below.
Operators on the same row associate left to right.

| Operators | Description |
| --- | --- |
| `.` `[]` `::` | field access, array element, PostgreSQL-style cast |
| `->` `->>` `#>` `#>>` | JSON-style field and path access |
| `+` `-` | unary plus and minus |
| `*` `/` `%` | multiplication, division, modulo |
| `+` `-` | addition, subtraction |
| `~` | bitwise not |
| `\|\|` `&` `\|` `#` `<<` `>>` | concatenation, bitwise and, or, xor and shifts |
| `BETWEEN` `IN` `LIKE` `LIKE_REGEX` `~` `~*` `!~` `!~*` `OVERLAPS` `CONTAINS` `PRECEDES` … | predicates, including the regex matches and the period predicates |
| `=` `<>` `!=` `<` `>` `<=` `>=` | comparisons, including `= ANY (…)` and `= ALL (…)` |
| `IS` | `IS NULL`, `IS TRUE`, `IS DISTINCT FROM` and their negations |
| `NOT` | logical negation |
| `AND` | logical conjunction |
| `OR` | logical disjunction |

So `'total: ' || x + 1` is `'total: ' || (x + 1)`, and `a = b LIKE 'x%'` is `a = (b LIKE 'x%')`.

This follows PostgreSQL's operator precedence, with two exceptions:

- The JSON-style access operators bind tighter than arithmetic, so `data -> 'n' + 1` adds one to the field rather than looking up `'n' + 1`.
- The regex operators bind as predicates alongside `LIKE`, so `a ~ 'x' || y` matches against `'x' || y`.

The following control structures are available in XTDB:

## `CASE`

`CASE` takes two forms:

1. With a `test-expr`, `CASE` tests the result of the `test-expr` against each of the `` value-expr`s until a match is found - it then returns the value of the corresponding `result-expr ``.

    ``` sql
    CASE <test-expr>
      WHEN <value-expr> THEN <result-expr>
      [ WHEN ... ]
      [ ELSE <default-expr> ]
    END
    ```

    If no match is found, and a `default-expr` is present, it will
    return the value of that expression, otherwise it will return null.

2. With a series of predicates, `CASE` checks the value of each `predicate` expression in turn, until one is true - it then returns the value of the corresponding `result-expr`.

    ``` sql
    CASE
      WHEN <predicate> THEN <result-expr>
      [ WHEN ... ]
      [ ELSE <default-expr> ]
    END
    ```

    If none of the predicates return true, and a `default-expr` is
    present, it will return the value of that expression, otherwise it
    will return null.

## `COALESCE` / `NULLIF`

`COALESCE` returns the first non-null value of its arguments:

``` sql
COALESCE(<expr>, ...)
```

`NULLIF` returns null if `expr1` equals `expr2`; otherwise it returns the value of `expr1`.

``` sql
NULLIF(<expr1>, <expr2>)
```
