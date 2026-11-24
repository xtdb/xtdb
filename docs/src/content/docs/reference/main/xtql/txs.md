---
title: XTQL Transactions (Clojure)
---

<details>
<summary>Changelog (last updated v2.2.1)</summary>

v2.2.1: a `put-docs` without its key into a table not yet written to aborts its transaction

: A `put-docs` document carries the table's [primary key](/reference/main/sql/schema#primary-keys-v221), `:xt/id` unless the table declares another.
  Into a table that has been written to, a document without it fails at submit; into a table that hasn't, the transaction aborts with `xtdb.tx/missing-id` — see [`put-docs`](#put-docs).

  Previously every such document failed at submit, with the error code `missing-id`.

  A client of `submit-tx` putting into a table that hasn't been written to no longer sees the error when it submits: check the transaction's outcome, or use `execute-tx`.

  The key's value is checked in the same places: one of a type that can't key a row (see [ID types](/reference/main/sql/txs#id-types)) fails at submit, or aborts the transaction, with `xtdb/invalid-id`.
  Previously a `put-docs` failed at submit with `xtdb.tx/invalid-eid` for any `:xt/id` of such a type; in a table keyed on another column, `:xt/id` is now an ordinary column, and takes any value.

v2.2.1: `patch-docs` patches the row identified by the table's key

: A `patch-docs` document carries the table's [primary key](/reference/main/sql/schema#primary-keys-v221), `:xt/id` unless the table declares another, and patches the row that key identifies — see [`patch-docs`](#patch-docs).
  A document without it fails as a `put-docs` document does.

  Previously every `patch-docs` document had to carry `:xt/id`, which identified its row whatever the table was keyed on.

v2.2.1: `delete-docs` and `erase-docs` take the table's key values

: In a table keyed on another column, `delete-docs` and `erase-docs` remove the rows whose key has the given values — see [`delete-docs`](#delete-docs).

  Previously they removed the rows whose `:xt/id` had the given values, whatever the table was keyed on.

  In a table keyed on several columns, each document is identified by a vector of its key values, in key order: `[:delete-docs :t [1 2] [3 4]]` deletes two documents.

</details>

Transactions in XTDB are submitted to the [log](/ops/config/log), to be processed asynchronously.
They each consist of an array of [operations](#tx-ops).

This document provides examples for EDN transaction operations, to be submitted to [`xt/execute-tx`](/drivers/clojure/codox/xtdb.api.html#var-execute-tx) or [`xt/submit-tx`](/drivers/clojure/codox/xtdb.api.html#var-submit-tx).

## Transaction operations

### `put-docs`

Upserts documents into the given table, optionally during the given valid time period.

``` clojure
[:put-docs
 ;; -- required

 ;; options map
 ;; * can just provide `<table>` rather than a map if there are
 ;;   no other options
 {;; -- required

  ;; table to put docs into (keyword)
  :into <table>

  ;; --optional

  ;; valid-from, valid-to can be `java.util.Date`, `java.time.Instant`
  ;; or `java.time.ZonedDateTime`
  :valid-from #inst "..."
  :valid-to #inst "..."
  }

 ;; -- required
 ;; documents to submit (variadic, 0..n)
 ;; * each must contain the table's primary key -
 ;;   `:xt/id` unless the table declares another
 & <docs>
 ]
```

A document missing any column of the table's [primary key](/reference/main/sql/schema#primary-keys-v221) is an `xtdb.tx/missing-id` error.
For a table that has been written to, `submit-tx` throws it; for one that hasn't, whose key a transaction ahead of this one may still declare, the transaction aborts with it.

#### Examples

- single document

    ``` clojure
    [:put-docs :my-table {:xt/id :foo}]
    ```

- with options

    ``` clojure
    [:put-docs {:into :my-table, :valid-from #inst "2024-01-01"}
     {:xt/id :foo, ...}
     {:xt/id :bar, ...}]
    ```

- dynamically generated

    ``` clojure
    (into [:put-docs {:into :my-table, ...}]
    (->> (range 100)
    (map (fn [n]
    {:xt/id n, :n-str (str n)}))))
    ```

### `patch-docs`

Upserts documents into the given table, merging them with any existing documents, optionally during the given valid time period.

Documents are currently merged at the granularity of individual keys - e.g. if a key is present in the patch document, it will override the same key in the database document; if a key is absent or null, the key from the document already in the database will be preserved.

``` clojure
[:patch-docs
 ;; -- required

 ;; options map
 ;; * can just provide `<table>` rather than a map if there are
 ;;   no other options
 {;; -- required

  ;; table to patch docs into (keyword)
  :into <table>

  ;; --optional

  ;; valid-from, valid-to can be `java.util.Date`, `java.time.Instant`
  ;; or `java.time.ZonedDateTime`
  :valid-from #inst "..."
  :valid-to #inst "..."
  }

 ;; -- required
 ;; documents to submit (variadic, 0..n)
 ;; * each must contain the table's primary key -
 ;;   `:xt/id` unless the table declares another
 & <docs>
 ]
```

Each document patches the row its [primary key](/reference/main/sql/schema#primary-keys-v221) identifies.
A document without the key is an `xtdb.tx/missing-id` error, raised as for [`put-docs`](#put-docs): at submit for a table that has been written to, otherwise by aborting the transaction.

#### Examples

- single document

    ``` clojure
    [:put-docs :my-table {:xt/id :foo, :a 1}]
    [:patch-docs :my-table {:xt/id :foo, :b 2}]

    ;; => {:xt/id :foo, :a 1, :b 2}
    ```

- with options

    ``` clojure
    [:patch-docs {:into :my-table, :valid-from #inst "2024-01-01"}
     {:xt/id :foo, ...}
     {:xt/id :bar, ...}]
    ```

- dynamically generated

    ``` clojure
    (into [:patch-docs {:into :my-table, ...}]
    (->> (range 100)
    (map (fn [n]
    {:xt/id n, :n-str (str n)}))))
    ```

### `delete-docs`

Deletes documents from the given table, optionally during the given valid time period.
The default valid time behaviour is the same as [put](#put-docs), above.

Each document is identified by its value of the table's [primary key](/reference/main/sql/schema#primary-keys-v221) — its `:xt/id`, unless the table declares another key.
In a table keyed on several columns, a document is identified by a vector of its key values, in the key's column order.
A value of a type that can't key a row (see [ID types](/reference/main/sql/txs#id-types)) fails at submit with `xtdb.tx/invalid-eid`.

:::caution
`delete-docs` and `erase-docs` take one key per document, so a composite key is one vector per document — not one value per key column.
In a table keyed on `(a, b)`, `[:delete-docs :t [1 2] [3 4]]` deletes two documents, `{:a 1, :b 2}` and `{:a 3, :b 4}`; `[:delete-docs :t 1 2]` deletes neither.

A key with the wrong number of values — a single value for a composite key, or a vector whose length isn't the key's — fails at submit with `xtdb.tx/key-arity` when submitted to an in-process node whose table has been written to.
Through a remote client (pgwire or FlightSQL) it isn't checked, and identifies no document: the transaction commits, and deletes nothing.

A vector's values identify a document only in the key's column order: in a table keyed on `(a, b)`, `[2 1]` identifies `{:a 2, :b 1}`, not `{:a 1, :b 2}`.
Values out of order aren't checked anywhere, and identify a different document, or none.
:::

``` clojure
[:delete-docs
 ;; -- required

 ;; options map
 ;; * can just provide `<table>` rather than a map if there are no other options
 {;; -- required

  ;; table to delete docs from
  :from <table>

  ;; --optional

  ;; valid-from, valid-to can be `java.util.Date`, `java.time.Instant` or `java.time.ZonedDateTime`
  :valid-from #inst "..."
  :valid-to #inst "..."
  }

 ;; -- required
 ;; keys of the documents to delete (variadic, 0..n)
 ;; * one per document: its key value, or for a table keyed on
 ;;   several columns, a vector of its key values in key order
 & <keys>
 ]
```

Examples:

- single document

    ``` clojure
    [:delete-docs :my-table :foo]
    ```

- with options

    ``` clojure
    [:delete-docs {:from :my-table, :valid-from #inst "2024-01-01"}
     :foo :bar ...]
    ```

- from a table keyed on several columns — one vector per document, in key order

    ``` clojure
    ;; CREATE TABLE orders (customer, order_no, PRIMARY KEY (customer, order_no))
    [:delete-docs :orders [:alice 1] [:bob 7]]
    ```

- dynamically generated

    ``` clojure
    (into [:delete-docs {:from :my-table, ...}]
    (range 100))
    ```

### `erase-docs`

Irrevocably erases documents from the given table (including through system time), for all valid-time.
Each document is identified by its value of the table's key, as for [`delete-docs`](#delete-docs).

``` clojure
[:erase-docs
 ;; -- required

 ;; table to erase documents from
 <table>

 ;; keys of the documents to erase (variadic, 0..n)
 ;; * one per document, as for `delete-docs`
 & <keys>
 ]
```

Examples:

- single document

    ``` clojure
    [:erase-docs :my-table :foo]
    ```

- from a table keyed on several columns

    ``` clojure
    [:erase-docs :orders [:alice 1] [:bob 7]]
    ```

- dynamically generated

    ``` clojure
    (into [:erase-docs :my-table] (range 100))
    ```

## Transaction options

Transaction options are an optional map of the following keys:

``` clojure
{;; -- optional
 :system-time #inst "2024-01-01"
 :default-tz #xt/zone "America/Los_Angeles"}
```
