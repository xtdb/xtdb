---
title: Clojure SQL Cookbook
---

SQL queries are submitted through `xtdb.api/q`:

- `(xt/q <node> <query> <opts>?)` returns the query results as a vector of maps.
    - `opts`: map of query options
        - `:args`: vector of query arguments
        - `:snapshot-token`, `:current-time`, `:tx-timeout` : see
    [XTQL](/reference/main/xtql/queries#basis)

- `(xt/q& <node> <query> <opts>?)`: returns a `CompletableFuture` of the query results.

For example:

``` clojure
(xt/q node "SELECT u.first_name, u.last_name FROM users u WHERE _id = ?"
      {:args ["James"]})
```

## SQL Transactions

SQL transactions are submitted through `xtdb.api/execute-tx` and `xtdb.api/submit-tx`.

- `(xt/submit-tx <node> <tx-ops> <opts>?)`: returns the transaction key of the submitted transaction.
    - `tx-ops`: vector of [transaction operations](#tx-ops).
    - `opts` (map):
        - `:default-tz` (`java.time.ZoneRegion`): time zone to be used
    by default in functions where no explicit override is
    provided. Defaults to the current TZ of the server JVM.

- `(xt/execute-tx <node> <tx-ops> <opts>?)` : additionally awaits for the transaction to be processed, and throws if the transaction fails (either through error, or assertion failure).

SQL transaction operations are of the form `[:sql "<sql query>"]`.

e.g.

``` clojure
(require '[xtdb.api :as xt])

(xt/execute-tx node [[:sql "INSERT INTO users (_id, name) VALUES ('jms', 'James')"]

                     ;; with args - pass multiple vectors if required.
                     [:sql "INSERT INTO users (_id, name) VALUES (?, ?)"
                      ["jms", "James"]
                      ["jdt", "Jeremy"]]])

;; => {:tx-id 0, :system-time #xt/instant "...", :committed? true}
```

:::note
There is a table and column name mapping between SQL and XTQL: documents inserted with XTQL have their hyphens translated to underscores, and their namespace segments converted to `$` symbols, as hyphens, periods and slashes are not valid symbols in SQL identifiers.

For example, `:foo.bar/baz-quux` in XTQL is referenced in SQL as `foo$bar$baz_quux`.

The built-in XTDB columns `:xt/id`, `:xt/valid-from`, `:xt/valid-to` etc are referenced in SQL as `_id`, `_valid_from` and `_valid_to` respectively.

This mapping is reversed when querying SQL documents from XTQL.

For more details on XTDB's SQL support, see the [SQL reference documentation](/reference/main/sql/queries).

:::
