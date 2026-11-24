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

A document without the table's [primary key](/reference/main/sql/schema#primary-keys-v221) is an `xtdb.tx/missing-id` error.
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
 ;; document ids to delete (variadic, 0..n)
 & <ids>
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

- dynamically generated

    ``` clojure
    (into [:delete-docs {:from :my-table, ...}]
    (range 100))
    ```

### `erase-docs`

Irrevocably erases documents from the given table (including through system time), for all valid-time.

``` clojure
[:erase-docs
 ;; -- required

 ;; table to erase documents from
 <table>

 ;; document ids to erase (variadic, 0..n)
 & <ids>
 ]
```

Examples:

- single document

    ``` clojure
    [:erase-docs :my-table :foo]
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
