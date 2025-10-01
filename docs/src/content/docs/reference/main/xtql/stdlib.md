---
title: Standard library
---

XTQL uses the same [standard library](/reference/main/stdlib.html) as SQL, written in s-expression form.

The following notes and exceptions apply:

- [Predicates](/reference/main/stdlib/predicates):
    - XTQL predicates are variadic, with evaluation rules as per Clojure.
    - `expr1 <> expr2` → `(not= exprs...)` (in addition to `(<> exprs...)` and `(!= exprs...)`)
    - `expr IS TRUE` → `(true? expr)`
    - `expr IS FALSE` → `(false? expr)`
    - `expr IS NULL` → `(nil? expr)`
- [String functions](/reference/main/stdlib/string):
    - `like-regex` accepts Clojure regex reader syntax (e.g. `#".foo."`)
    - `str LIKE pattern` → `(like str pattern)`
    - `str LIKE_REGEX pattern [FLAG flags]` → `(like-regex str regex <flags>)`
    - `OVERLAY` → `(overlay str1 replacement start <length>)`
    - `TRIM(BOTH <trim_char> FROM str)` → `(trim str <trim-char>)`
    - `TRIM(LEADING <trim_char> FROM str)` → `(trim-leading str <trim-char>)`
    - `TRIM(TRAILING <trim_char> FROM str)` → `(trim-trailing str <trim-char>)`
- [Temporal functions](/reference/main/stdlib/temporal):
    - Function names are in kebab-case, e.g.:
        - `p1 CONTAINS p2` → `(contains? p1 p2)`
        - `p1 STRICTLY CONTAINS p2` → \`(strictly-contains? p1 p2)
    - `DATE_TRUNC('UNIT', date_time)` → `(date-trunc "UNIT" date-time)`
    - `EXTRACT('FIELD' FROM date_time)` → `(extract "FIELD" date-time)`

## Control structures

The following control structures are available in XTDB:

### if

``` clojure
(if <predicate>
  <then-expr>
  <else-expr>)
```

- If the predicate evaluates to `true`, returns the value of `then-expr`, else returns the value of `else-expr`.
- The predicate must return a boolean value.
  To turn any value into a boolean use the `boolean` function, which returns `false` if its argument is false, null or absent; otherwise it returns `true`.

### case / cond

`case` tests the result of the `test-expr` against each of the `` value-expr`s until a match is found - it then returns the value of the corresponding `result-expr ``.

``` clojure
(case <test-expr>
  <value-expr> <result-expr>
  ...

  <default-expr>?)
```

- If no match is found, and a `default-expr` is present, it will return the value of that expression.

`cond` checks the value of each `predicate` expression in turn, until one is `true` - it then returns the value of the corresponding `result-expr`

``` clojure
(cond
  <predicate> <result-expr>
  ...
  <default-expr>?)
```

- If none of the predicates return true, and a `default-expr` is present, it will return the value of that expression.
- Clojure users: note that there's no `:else` required in the default expression.

### let

Returns the value of `body-expr`, with `symbol` bound to the result of `binding-expr`.

``` clojure
(let [<symbol> <binding-expr>]
  <body-expr>)
```

- Only one symbol/binding pair is permitted.

### if-some

If `binding-expr` returns a non-null value, returns the result of `then-expr` with `symbol` bound to the result of the binding expression.
Otherwise, returns the value of `else-expr`.

``` clojure
(if-some [<symbol> <binding-expr>]
  <then-expr>
  <else-expr>)
```

### coalesce / null-if

`coalesce` returns the first non-null value of its arguments:

``` clojure
(coalesce <expr>*)
```

`null-if` returns null if `expr1` equals `expr2`; otherwise it returns the value of `expr1`.

``` clojure
(null-if <expr1> <expr2>)
```
