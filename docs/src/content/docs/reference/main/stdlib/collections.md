---
title: Collection functions
---

:::note
- SQL array subscripts are 1-based.
- XTQL `nth` indexes are 0-based.
- Unless otherwise specified, if any argument is null, the result will be null.
:::

`CARDINALITY(list)`
: returns the number of elements in the list.

`TRIM_ARRAY(array, n)`
: returns a copy of `array` with the last `n` elements removed.

`array[idx]` | XTQL `(nth array idx)`
: returns the element at the given index.
  - In SQL, array subscripts are 1-based, so `ARRAY[10, 20, 30][1]` returns `10`.
  - In XTQL, `nth` is 0-based, so `(nth [10 20 30] 0)` returns `10`.
  - Returns `NULL` / `nil` if `idx` is null, negative, out of bounds, or the input value is not a list.

Examples:

```sql
SELECT ARRAY[10, 20, 30][1] AS first_val
-- 10
```

```clojure
(nth [10 20 30] 0)
;; 10
```

```clojure
(nth [10 20 30] 5)
;; nil
```
