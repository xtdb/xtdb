---
title: Standard Library
---

XTDB provides a rich standard library of predicates and functions:

- [Predicates](/reference/main/stdlib/predicates)
- [Numeric functions](/reference/main/stdlib/numeric)
- [String functions](/reference/main/stdlib/string)
- [Temporal functions](/reference/main/stdlib/temporal)
- [Aggregate functions](/reference/main/stdlib/aggregates)
- [Other functions](/reference/main/stdlib/other)

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
