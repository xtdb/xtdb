---
title: XTQL Queries
---

XTQL queries consist of composable operators, optionally combined with a pipeline.

## Operators

- [Source operators](#source-operators) are valid at the start of a pipeline, or in isolation.
- [Tail operators](#tail-operators) transform data in a pipeline - they aren't valid as the first operator, because they don't source data, but can appear anywhere else in the pipeline.

A pipeline consists of a source operator, and optionally many tail operators:

``` clojure
(-> (from ...)     ; source
    (order-by ...) ; tail
    (limit ...)    ; tail
    )

;; unlike in Clojure, XTQL's `->` isn't a threading macro
;; - just the symbol for a pipeline of operations.
```

Pipelines are optional - queries with just a source operator (i.e. no tails) can be submitted simply as:

``` clojure
(from ...)
```

    Query :: (fn [Param*] Pipeline) | Pipeline
    Param :: Symbol

    Pipeline :: (-> Source Tail*) | Source
    Source :: From | Rel | Unify
    Tail :: Aggregate | Limit | Offset | OrderBy | Return
    | Where | With | Without | Unnest

### Source operators

| Operator | Purpose |
| --- | --- |
| [From](#from) | Sources data from a table in XTDB |
| [Relation](#relation) | Sources data from the user-specified relation |
| [Unify](#unify) | Combines multiple input sources using Datalog-style unification |


### Tail operators

| Operator | Purpose |
| --- | --- |
| [Aggregate](#aggregate) | Groups the relation rows by the given aggregate-specs |
| [Limit](#limit) | Only returns the top N rows of the relation |
| [Offset](#offset) | Skips the first N rows of the relation |
| [Order by](#order-by) | Orders the relation by the given columns |
| [Return](#return) | Restricts the output to the given columns |
| [Where](#where) | Filters the relation using the given predicates |
| [With](#with) | Adds columns to the relation |
| [Without](#without) | Removes columns from the relation |
| [Unnest](#unnest) | Flattens an array within a column into individual rows |


### Unify clauses

Joins in XTQL are specified using the ['unify'](#unify) operator - this combines multiple input relations using [Datalog-style unification](#unify_explanation).
This allows for very declarative yet terse method of specifying join conditions; how relations relate to each other.

'Unify clauses' are inputs to the unify operator.
They are a selection of [source](#source-operators) and [tail](#tail-operators) operators with a few extra operators that are only valid in unification.

| Clause | Purpose |
| --- | --- |
| [From](#from) | Sources data from a table in XTDB |
| [Join](#joins) | Further constrains the unification using the given query |
| [Left Join](#joins) | Optionally joins the unification against the given query |
| [Rel](#relation) | Sources data from the user-specified relation |
| [Unnest](#unnest) | Flattens an array within a column into individual rows |
| [Where](#where) | Filters the rows using the given predicates |
| [With](#with) | Defines new logical variables in the unification |


### Aggregate

The 'aggregate' operator aggregates rows in a query according to a list of aggregate specs.
An aggregate spec is either a grouping variable/column or (new) column plus an [expression](#expressions).

The available aggregate functions are documented [here](../stdlib/aggregates).

    Aggregate :: (aggregate AggSpec*)

    AggSpec :: GroupingVar | {Column Expr, ...}

    GroupingVar :: symbol
    Column :: keyword

    Expr :: <defined separately>

``` clojure
(-> (unify (from :customers [{:xt/id customer-id, :name customer-name}])
           (from :orders [customer-id order-value]))
    (aggregate customer-id customer-name
               {:order-count (row-count)
                :total-value (sum order-value)}))
```

### From

The 'from' operator sources data from a table in XTDB - it expects the table to fetch from, as well as options that define what columns to return, and optionally any temporal filters to apply.

The binding specs define which columns are retrieved from the table, and specify constraints on those columns.
For more details, see the [binding specs](#binding-specs) section.

For example:

    From :: (from Table FromOpts)
    Table :: keyword

    FromOpts :: [BindSpec+]
    | {; required
    :bind [BindSpec+]

    ; optional
    :for-valid-time TemporalFilter
    :for-system-time TemporalFilter}

``` clojure
;; `SELECT username, first_name, last_name FROM users`
(from :users [username first-name last-name])

;; `SELECT username AS login, first_name, last_name FROM users`
(from :users [{:username login} first-name last-name])

;; `SELECT first_name, last_name FROM users WHERE username = 'james'`
(from :users [{:username "james"} first-name last-name])

;; `SELECT first_name, last_name FROM users WHERE username = ?`
(from :users [{:username $username} first-name last-name])
```

Additionally, 'from' supports a special column reference - `projectAllCols` in JSON and the `*` symbol in Clojure.
Like in SQL, this can be used to specify that all columns of a given table are to be projected out.

``` clojure
;; `SELECT * FROM users`
(from :users [*])

;; `SELECT *, username AS login FROM users`
(from :users [* {:username login}])
```

:::caution
Note that, due to the implicit unification properties of 'from' outlined in the [binding specs](#binding-specs) section, explicitly projected columns will unify with those projected out as a result of `projectAllCols`/`*`.

It is due to this property of implicit unification and projection that `projectAllCols`/`*` as a column reference in 'from' is not supported within a unification context.

``` clojure
;; INVALID
(unify (from :users [*])
       (from :customers [*]))
```
:::

#### Temporal filters

Temporal filters control the document versions that are visible to the query.

- `at <timestamp>`: rows that were/will be visible at the specified timestamp - i.e. `row-from <= timestamp < row-to`
- `from <timestamp>`: rows that have been visible any time after the timestamp - i.e. `row-to > timestamp`
- `to <timestamp>`: rows that were visible any time before the timestamp - i.e. `row-from < timestamp`
- `in <from-timestamp> <to-timestamp>`: rows that were visible any time within the period - i.e.
    `row-to > <from-timestamp> && row-from < <to-timestamp>`

- `all-time`: all rows, throughout history.

Unless otherwise specified, queries will see the current version of the row, `at <now>`, in both valid time and system time.

    TemporalFilter :: (at Timestamp)
    | (from Timestamp)
    | (to Timestamp)
    | (in Timestamp Timestamp)
    | :all-time

    Timestamp :: java.util.Date | java.time.Instant | java.time.ZonedDateTime

``` clojure
(from :users {:bind [...]
              :for-valid-time (in #inst "2020-01-01" #inst "2021-01-01")
              :for-system-time (at #inst "2023-01-01")})
```

Without any temporal filters, it is valid to just specify the binding specs without a map.

### Joins - join, left join

The 'join' and 'left join' [unify clauses](#unify-clauses) further constrain a unification by joining against the given query.

We join the inner query to the rest of the unify inputs using the binding specs - see the [binding specs](#binding-specs) section for more details.
These binding specs act as both 'join conditions' (if the logic variables are reused within the [unify](#unify) operator) and a specification of which columns from the sub-query should be returned from the outer query.

- The 'join' operator performs an inner, or required, join with the sub-query - if a row from the outer query doesn't match, it won't be returned
- The 'left-join' operator performs an outer, or optional, join with the sub-query - if a row from the outer query matches, it'll be returned; if it doesn't, it will still be returned, but with null values in the sub-query columns.

Parameters in the sub-query can be fulfilled by passing a vector of arguments or, if the symbols all match, the arguments may be omitted - see the [arguments](#arguments) section for more details.

    Join :: (join Subquery [BindSpec+])
    LeftJoin :: (left-join Subquery [BindSpec+])

``` clojure
(unify (from :customers [{:xt/id customer-id} customer-name]
       (left-join (from :orders [{:xt/id order-id}, customer-id, order-value])
                  [customer-id order-id order-value])))
```

In this case, `customer-id` is specified multiple times, so this adds a join-condition constraint; `order-id` and `order-value` are not specified elsewhere within the unify, so these columns are simply returned.

### Limit

The 'limit' operator limits the rows returned by the query.
Without an explicit preceding [order by](#order-by), the rows selected for return are undefined.

    Limit :: (limit LimitN)
    LimitN :: non-negative integer

``` clojure
(-> (from :users [username])
    (order-by username)
    (limit 10))
```

### Offset

The 'offset' operator skips the first N rows that would have otherwise been returned by the query.
Without an explicit preceding [order by](#order-by), the rows selected for return are undefined.

For example:

    Offset :: (offset OffsetN)
    OffsetN :: non-negative integer

``` clojure
(-> (from :users [username])
    (order-by username)
    (offset 10)
    (limit 10))
```

### Order by

The 'order by' operator sorts the rows in a relation.
It takes a collection of order specs.
An order spec is either a simple column to sort by (default descending) or a composite object of an expression to sort by, a direction and a default null ordering.
When multiple order spec are supplied priority is given from left to right.

    OrderBy :: (order-by OrderSpec+)
    OrderSpec :: OrderCol
    | {; required
    :val Expr

    ; optional
    :dir Direction
    :nulls NullOrdering}

    OrderCol :: symbol
    Direction :: :asc | :desc
    NullOrdering :: :first | :last
    Expr :: <defined separately>

``` clojure
;; sort by order-value descending, with nulls returned last,
;; then received-at ascending
(-> (from :orders [order-value received-at])
    (order-by {:val order-value, :dir :desc, :nulls :last}
              received-at))
```

### Return

The 'return' operator specifies the columns to return from the query.
It also allows additional projections, should you want to return a new column based on existing columns.

If you want to introduce a projected column while keeping the existing columns see the [with](#with) operator.

    Return :: (return ReturnSpec*)
    ReturnSpec :: ReturnVar | {Column Expr, ...}
    ReturnVar :: symbol
    Column :: keyword
    Expr :: <defined separately>

``` clojure
(-> (from :users [username first-name last-name])
    (return username {:full-name (concat last-name ", " first-name)}))

;; =>

[{:username "...", :full-name "..."}
 ...]
```

### Rel(ation)

The 'rel' operator creates an inline relation with the provided values.
The first argument is an array of maps, either as a literal, a parameter, or a value nested within another document.
The 'rel' operator yields each element as a row, with the values in the map [bound/constrained](#binding-specs) as required.

- To unwrap an array of values rather than an array of maps, with a variable bound to each row instead, see [`unnest`](#unnest).

<!-- -->

    Rel :: (rel RelExpr [BindSpec+])
    RelExpr :: Expr

    Expr :: <defined separately>

``` clojure
;; as a literal
(rel [{:a 1, :b 2}, {:a 3, :b 4}] [a b])

;; from a parameter
(xt/q node ['#(rel % [a b])
            [{:a 1, :b 2}, {:a 3, :b 4}]])

;; from a value in another document
;; assume we have a document {:xt/id <id>, :my-nested-rel [{:a 1, :b 2}, ...]}
(-> (from :docs [my-nested-rel])
    (rel my-nested-rel [a b]))

;; same, but within a `unify`
(unify (from :docs [my-nested-rel])
       (rel my-nested-rel [a b]))
```

### Unify

The 'unify' operator combines multiple input relations using Datalog-style unification (explained below), to achieve join-like behaviour.

Each input relation defines a set of 'logic variables' in its binding specs - if a logic variable appears more than once within a single `unify` operator, the results are constrained such that the logic variable has the same value everywhere it's used.
This has the effect of imposing 'join conditions' over the inputs.

    Unify :: (unify UnifyClause+)
    UnifyClause :: From | Join | LeftJoin | Rel | Where | With

``` clojure
(unify (from :customers [{:xt/id customer-id} customer-name])
       (from :orders [{:xt/id order-id} customer-id order-value]))
```

Because this query uses the `customer-id` logic variable twice, we add a constraint that the two occurrences must be equal - it's therefore equivalent to the following SQL:

``` sql
SELECT c._id AS customer_id, customer_name,
       o._id AS order_id, o.order_value
FROM customers c
  JOIN orders o ON (c._id = o.customer_id)
```

- In [rel](#relation) and [from](#from) clauses any logic variables specified in its binding specs are unified.
- [Join](#joins) and [left join](#joins) clauses work in a similar way to [from](#from), except they execute a full sub-query (e.g.
    another pipeline) rather than reading a single table. Any logic variables specified in their binding specs are unified in the same way.

- [Where](#where) clauses further constrain the results using predicates - these have access to any logic variable bound in the containing unify operator.
- [With](#with) clauses within unify may define additional logic variables or, if these logic variables are used elsewhere, the value of the [with](#with) result must agree with the value elsewhere in the unify.
- The unify operator returns a relation containing a column for every logic variable bound in any of its clauses.

### Unnest

The 'unnest' operator extracts values from an array - returning one row for each element.
The other columns in the query are duplicated for each row.

- To unwrap an array of maps (a relation) rather than an array of values, with a variable bound to each map-key instead, see [rel](#relation).
- If the value in question isn't an array, or the array is empty, the row is filtered out.

<!-- -->

    Unnest :: (unnest UnnestSpec)

    ; as a tail operator
    UnnestSpec :: {Column Expr}
    Column :: keyword

    ; in `unify`
    UnnestSpec :: {LogicVar Expr}
    LogicVar :: symbol

    Expr :: <defined separately>

``` clojure
;; as a 'tail' operator - N.B. `:tag` is a column being added
(-> (from :posts [{:xt/id post-id} tags])
    (unnest {:tag tags}))

;; in `unify` - N.B. `tag` is a logic var being introduced
(unify (from :posts [{:xt/id post-id} tags])
       (unnest {tag tags}))

;; =>

[{:post-id 1, :tag "sport"}
 {:post-id 1, :tag "formula-1"}
 {:post-id 2, :tag "health"}
 {:post-id 4, :tag "technology"}
 {:post-id 4, :tag "ai"}
 {:post-id 4, :tag "politics"}]
```

### Where

The 'where' operator filters rows in a query or unification operator.
It expects (optionally) many [predicates](/reference/main/stdlib/predicates) - rows that match all of the predicates will be returned; rows that fail to match one or more will be filtered out.

- Like all other XTQL expressions, `where` respects 'three-valued logic' - if an expression returns either false or null, the row will be filtered out.
- `where` is short-circuiting - if an earlier predicate doesn't return true for a row, the remaining predicates won't be evaluated.

<!-- -->

    Where :: (where Expr*)

    Expr :: <defined separately>

``` clojure
;; as a 'tail' operator
(-> (from :users [username date-of-birth])
    (where (> (current-timestamp)
              (+ date-of-birth #xt/period "P18Y"))))

;; in `unify`
(unify (from :customers [{:xt/id customer-id} customer-name vip?])
       (from :orders [{:xt/id order-id} customer-id order-value])
       (where (or vip? (> order-value 1000000))))
```

### With

The 'with' operator specifies columns to add to the query.
It takes a collection of with specs.
A with spec takes a column name (in the pipeline context) or a logic var (in the unify context) and an [expression](#expressions) to bind that column/logic var to.

    With :: (with WithSpec*)

    ; as a tail operator
    WithSpec :: WithVar | {Column Expr, ...}

    ; in `unify`
    WithSpec :: WithVar | {LogicVar Expr, ...}

    WithVar :: symbol
    Column :: keyword
    LogicVar :: symbol

    Expr :: <defined separately>

``` clojure
;; as a 'tail' operator - N.B. `:full-name` is a column here
(-> (from :users [username first-name last-name])
    (with {:full-name (concat last-name ", " first-name)}))

;; in 'unify' - N.B. `full-name` is a logic variable here
(unify (from :users [username first-name last-name])
       (with {full-name (concat last-name ", " first-name)}))

;; =>

[{:username "...", :first-name "...", :last-name "...", :full-name "..."}
 ...]
```

### Without

The 'without' operator removes columns from the ongoing query:

For example, in this query, we only want the `customer-id` to join on - we don't want it returned - so we exclude it in a `without` operator.

    Without :: (without Column*)
    Column :: keyword

``` clojure
(-> (unify (from :customers [{:xt/id customer-id}, customer-name])
           (from :orders [customer-id order-value]))
    (without :customer-id))
```

## Expressions

XTQL expressions are valid within predicates, projections, bindings and arguments.

- Call expressions can use functions from the [XTDB standard library](../stdlib).
- Variable expressions can refer to any variable in scope - within a `unify` clause, any logic variable; within any other operator, any column returned in the previous step.

### Subqueries

- Subquery expressions must return a single row containing a single column - otherwise, a runtime exception will be thrown.
- 'Exists' expressions will return false if the subquery returns no rows; true otherwise.
- 'Pull' expressions must return a single row - otherwise, a runtime exception will be thrown.
  The columns in the returned row will be nested into a map in the outer expression.
- 'Pull many' expressions may return any number of rows.
  The rows will be nested into an array of maps in the outer expression.
- The arguments to sub-queries are referred to as parameters in the inner query; no other variables from the outer scope are available in the inner query.

<!-- -->

    Expr :: number | "string" | true | false | nil | ObjectExpr
    | SetExpr | [Expr*] | {MapKey Expr, ...}
    | ParamExpr | VariableExpr
    | GetFieldExpr | CallExpr
    | SubqueryExpr | ExistsExpr | PullExpr | PullManyExpr

    ObjectExpr :: java.time.Temporal | java.time.TemporalAmount

    SetExpr :: #{Expr*}
    VectorExpr :: [Expr*]
    MapExpr :: {MapKey Expr, ...}
    MapKey :: keyword

    ParamExpr :: symbol
    VariableExpr :: symbol
    GetFieldExpr :: (. Expr symbol)
    CallExpr :: (symbol Expr*)

    SubqueryExpr :: (q Subquery)
    ExistsExpr :: (exists Subquery)
    PullExpr :: (pull Subquery)
    PullManyExpr :: (pull* Subquery)

The following example retrieves a post together with their author and comments:

``` clojure
(fn [post-id]
  (-> (from :posts [{:xt/id post-id} post-content author-id])
      (with {:author (pull (from :authors [{:xt/id author-id} first-name last-name])
                           {:args [author-id]})

             :comments (pull* (-> (from :comments [{:post-id post-id} comment posted-at])
                                  (order-by posted-at)
                                  (limit 2)
                                  (return comment))
                              {:args [{:post-id post-id}]})})

      (return post-content author comments)))

;; =>

{:post-content "..."
 :author {:name "..."}
 :comments [{:comment "..."}, {:comment "..."}]}
```

## Binding specs

Binding specs define which columns are retrieved from a relation, and specify constraints on those columns.

    BindSpec :: BindVariable | {BindColumn Expr, ...}
    BindVariable :: symbol
    BindColumn :: keyword
    Expr :: <defined separately>

- We can retrieve columns by listing them:

    ``` clojure
    (from :users [username first-name last-name])

    ;; i.e. `SELECT username, first_name, last_name FROM users`
    ```

- We can rename columns by specifying a mapping:

    ``` clojure
    (from :users [{:username login} first-name last-name])

    ;; i.e. `SELECT username AS login, first_name, last_name FROM users`
    ```

- We can constrain rows by specifying literals or parameters:

    ``` clojure
    (from :users [{:username "james"} first-name last-name])

    ;; a query that takes one parameter, that we name `username`
    (fn [username]
      (from :users [{:username username} first-name last-name]))

    ;; using Clojure's `#()` syntax
    #(from :users [{:username %} first-name last-name])

    ;; i.e. `SELECT first_name, last_name FROM users WHERE username = 'james'`
    ;;      `SELECT first_name, last_name FROM users WHERE username = ?`
    ```

(In these examples, we use ['from'](#from) - but the same applies to ['join'](#joins) and ['left join'](#joins).)

Within unify operators, these output names (`first-name`, `last-name` etc.) create 'logic variables' which, if they are re-used within the same unify operator, will add a 'join condition' - see the [unify](#unify) operator for more details.

## Arguments

Arguments are used to pass values into a query, both for the query itself and for sub-queries.
By using parameters, we can create reusable queries that can be re-executed with different values.

Where [bindings](#binding-specs) specify how to join the **output** of the sub-query/join to the outer query, arguments specify the **inputs** to the sub-query/join from the outer query.

    Subquery :: [ Query Expr* ] | Query
    Expr :: <defined separately>

``` clojure
;; find the most recent 5 posts and, for each, their most recent 3 comments

(-> (from :posts [{:xt/id post-id} ...])
    (with {:comments (pull* [(fn [post-id]
                               (-> (from :comments [{:post-id post-id} comment commented-at])
                                   (limit 3)))

                             post-id])}))

;; in this query, the `post-id` argument is referenced as `post-id` in the sub-query

;; given the variable has the same name in the outer and inner query,
;; we can omit the application vector

(-> (from :posts [{:xt/id post-id} ...])
    (with {:comments (pull* (fn [post-id]
                              (-> (from :comments [{:post-id post-id} comment commented-at])
                                  (limit 3))))}))
```

As well as 'pull', this is quite commonly used in left joins, because we don't want to filter out rows that don't match (which would happen if the `<>` here was in the outer unify).

Instead, we want to preserve them, albeit without values for the columns in the right-hand side of the left-join.

``` clojure
;; find everybody and, for those who have them, their siblings

(-> (unify (from :people [{:xt/id person, :parent parent}])
           (left-join [(fn [person]
                         (-> (from :people [{:xt/id sibling, :parent parent}])
                             (where (<> person sibling))))
                        person]
                      [sibling parent]))
    (return person sibling))

;; in this query, the `person` argument is referenced as `person` in the sub-query;
;; `sibling` and `parent` are joined on the way out.

;; again, given the variable has the same name in the outer and inner query,
;; we can omit the application vector

(-> (unify (from :people [{:xt/id person, :parent parent}])
           (left-join (fn [person]
                        (-> (from :people [{:xt/id sibling, :parent parent}])
                            (where (<> person sibling))))
                      [sibling parent]))
    (return person sibling))
```

## Query options

XTQL query options are an optional map of the following keys:

`await-token`
: requires that the node has indexed *at least* as far as the
    specified await-token.

    - If not provided, XTDB clients will default it to the latest transaction submitted through that client.
      This is so that, by default, transactions submitted to a client are guaranteed to be visible to any later query to that same client.
    - If submitting transactions and queries to different clients (e.g. via a non-sticky load-balancer), it is the user's responsibility to pass the await-token returned after `submit-tx` as the `await-token` for subsequent queries to guarantee this same read-after-write consistency level.
    - If the requested transaction hasn't been indexed, the XTDB client will wait (see `tx-timeout`) before evaluating the query.

        ``` clojure
    (xt/q node ['#(from :users [{:username %}]) "james"])
        ```

`snapshot-token`
: a token that specifies the exact transactions that'll be visible to
    the query.

    - If the requested transaction hasn't been indexed, the XTDB client will wait (see `tx-timeout`) before evaluating the query.
    - If not provided, this will default to the latest available transaction on the node.

`current-time`
: overrides the wall-clock time used in any
    [functions](../stdlib/temporal#_current_time) that require it.

    - If not provided, defaults to the current wall-clock time of the executing node
    - In addition, when reading from tables, unless specified explicitly for an individual table, XTDB will also use this time as the valid-time to read the table at.

`default-tz`
: (defaults to JVM timezone on the executing node): the default
    timezone to use in [functions](../stdlib/temporal) that require it.

`explain?`
: rather than returning results, setting this flag to `true` returns
    the query plan for the query (default `false`).

`key-fn`
: specifies how keys are returned in query results.

    - `:kebab-case-keyword` (default): kebab-case, dot-namespaced keywords (e.g. `:foo.bar/baz-quux`)

`tx-timeout`
: duration to wait for the requested transaction (`await-token`) to be
    indexed before timing out (default unlimited).

These query options (in particular, `snapshot-token`, `current-time`, `default-tz` - together, the 'basis') allow for truly immutable, repeatable database snapshots - two queries run with the same basis will see exactly the same version of the whole database, regardless of any other transactions that have occurred in the meantime.
