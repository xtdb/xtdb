---
title: Introducing XTQL
---

XTDB is queryable using two query languages: **SQL** and **XTQL**.

XTQL is our new, data-oriented, composable query language:

- It is inspired by the strong theoretical bases of both **Datalog** and **relational algebra**.
  These two combine to create a joyful, productive, interactive development experience, with the ability to build queries iteratively, testing and debugging smaller parts in isolation.
- It is designed to be highly amenable to dynamic query generation - we believe that our industry has spent more than enough time trying to generate SQL strings (not to mention the concomitant [security vulnerabilities](https://owasp.org/www-community/attacks/SQL_Injection)).

## Querying XTQL

XTQL can either be queried within an SQL query, or via the Clojure API.

To query XTQL within a SQL query, you can either execute it:

- as a top-level query (like SQL's `VALUES`): `XTQL $$ <query> $$`:

    ``` sql
    XTQL $$
      (-> (from :users [first-name last-name])
    ...)
    $$
    ```

- or within a wider SQL query:

    ``` sql
    SELECT ...
    FROM (XTQL $$
      (-> (from :users [first-name last-name])
    ...)
    $$) u
    ORDER BY u.last_name DESC, u.first_name DESC
    LIMIT 10
    ```

## 'Operators' and 'relations'

XTQL is built up of small, composable 'operators', which combine together using 'pipelines' into larger queries.

- 'Source' operators (e.g. 'read from a table') each yield a 'relation' - an unordered bag of rows[^1].
- 'Tail' operators (e.g. 'filter a relation', 'calculate extra fields') transform a relation into another relation.

From these simple operators, we can build arbitrarily complex queries.

Our first operator is `from`:

### `from`

The `from` operator allows us to read from an XTDB table.
In this first example, we're reading the first-name and last-name fields from the `users` table - i.e. `SELECT first_name, last_name FROM users`:

``` clojure
(from :users [first-name last-name])
```

It's in the `from` operator that we specify the temporal filter for the table.
By default, this shows the table at the current time, but it can be overridden:

- to view the table at another point in time
- to view the changes to the table within a given range
- to view the entire history of the table

``` clojure
(from :users {:bind [first-name last-name]

              ;; at another point in time
              :for-valid-time (at #inst "2023-01-01")

              ;; within a given range
              :for-valid-time (in #inst "2023-01-01", #inst "2024-01-01")
              :for-valid-time (from #inst "2023-01-01")
              :for-valid-time (to #inst "2024-01-01")

              ;; for all time
              :for-valid-time :all-time

              ;; and all of the above :for-system-time too.
              })
```

In the `from` operator, we can also rename columns, and filter rows based on field values:

- We rename a column using a binding map:

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=bo-xtql-1,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=bo-sql-1,indent=0]
    ```

- We can look up a single user-id by specifying a literal in the binding map:

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=bo-xtql-2,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=bo-sql-2,indent=0]
    ```

Another source operator is `rel`, which allows you to specify an inline relation.

You can check out the [source operators reference](/reference/main/xtql/queries.html#source-operators) for more details.

### Pipelines

We can then transform the rows in a table using tail operators, which we pass in an operator 'pipeline'.
Pipelines consist of a single source operator, and then arbitrarily many tail operators.

Here, we demonstrate `SELECT first_name, last_name FROM users ORDER BY last_name, first_name LIMIT 10`, introducing the 'order by' and 'limit' operators:

In Clojure, we use `->` to denote a pipeline - in a similar vein to the threading macro in Clojure 'core' [^2], we take one source operator and then pass it through a series of transformations.

``` clojure
(-> (from :users [first-name last-name])
    (order-by last-name first-name)
    (limit 10))
```

By building queries using pipelines, we are now free to build these up incrementally, trivially re-use parts of pipelines in different queries, or temporarily disable some operators to test parts of the pipeline in isolation.

Other tail operators include `where` (to filter rows), `return` (to specify the columns to output), `with` (to add additional columns based on the existing ones), and `aggregate` (grouping rows - counts, sums, etc). For a full list, see the [tail operators reference](/reference/main/xtql/queries.html#tail-operators).

### Multiple tables - introducing `unify`

Joining multiple tables in XTQL is achieved using Datalog-based 'unification'.

We introduce the `unify` source operator, which takes an unordered bag of input relations and joins them together using 'unification constraints' (similar to join conditions).

Each input relation (e.g. `from`) defines a set of 'logic variables' in its bindings.
If a logic variable appears more than once within a single unify clause, the results are constrained such that the logic variable has the same value everywhere it's used.
This has the effect of imposing 'join conditions' over the inputs.

- In this case, we re-use the `user-id` logic variable to indicate that the `:xt/id` from the `:users` table should be matched with the `:author-id` of the `:articles` table.

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=joins-xtql-1,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=joins-sql-1,indent=0]
    ```

- For non-equality cases, we can use a [`where`](../reference/main/xtql/queries.html#where) clause (where we have a full SQL-inspired expression standard library at our disposal)

    ``` clojure
    ;; 'find me all the users who are the same age'
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=joins-xtql-2,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=joins-sql-2,indent=0]
    ```

- We can specify that a certain match is optional using [`left-join`](/reference/main/xtql/queries.html#joins):

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=joins-xtql-3,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=joins-sql-3,indent=0]
    ```

    Here, we're asking to additionally return customers who haven't yet
    any orders (for which the order-table columns will be absent in the
    results).

- Or, we can specify that we only want to return customers who *don't* have any orders, using [`not`](/reference/main/stdlib/predicates.html#boolean-functions) [`exists?`](/reference/main/xtql/queries.html#subqueries):

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=joins-xtql-4,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=joins-sql-4,indent=0]
    ```

The `unify` operator accepts 'unify clauses' - e.g. `from`, `where`, `with`, `join`, `left-join` - a full list of which can be found in the [unify clause reference guide](/reference/main/xtql/queries.html#unify-clauses).

### Projections

- We can create new columns from old ones using [`with`](../reference/main/xtql/queries.html#with):

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=proj-xtql-1,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=proj-sql-1,indent=0]
    ```

    We can also use [`with`](../reference/main/xtql/queries.html#with)
    within [`unify`](../reference/main/xtql/queries.html#unify) - this
    creates new logic variables which we can then unify in the same way.

- Where [`with`](../reference/main/xtql/queries.html#with) adds to the available columns, [`return`](../reference/main/xtql/queries.html#return) only yields the specified columns to the next operation:

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=proj-xtql-2,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=proj-sql-2,indent=0]
    ```

- Where we don't need any additional projections, we can use [`without`](../reference/main/xtql/queries.html#without):

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=proj-xtql-3,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=proj-sql-3,indent=0]
    ```

### Aggregations

To count/sum/average values, we use [`aggregate`](../reference/main/xtql/queries.html#aggregate):

``` clojure
Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=aggr-xtql-1,indent=0]
```

``` sql
Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=aggr-sql-1,indent=0]
```

### 'Pull'

When we've found the documents we're interested in, it's common to then want a tree of related information.
For example, if a user is reading an article, we might also want to show them details about the author as well as any comments.

(Users of existing EDN Datalog databases may already be familiar with ['pull'](../reference/main/xtql/queries.html#subqueries) - in XTQL, because subqueries are a first-class concept, we rely extensively on these to express a more powerful/composable behaviour.)

``` clojure
Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=pull-xtql-1,indent=0]

;; => [{:title "...", :content "...",
;;      :author {:first-name "...", :last-name "..."}
;;      :comments [{:comment "...", :name "..."}, ...]}]
```

``` sql
-- using XTDB's 'NEST_ONE'/'NEST_MANY'

Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=pull-sql-1,indent=0]
```

In this example, we use [`pull`](../reference/main/xtql/queries.html#subqueries) to pull back a single map - we know that there's only one author per article (in our system).
When it's a one-to-many relationship, we use [`pull*`](../reference/main/xtql/queries.html#subqueries) - this returns any matches in a vector.

Also note that, because we have the full power of subqueries, we can express requirements like 'only get me the most recent 10 comments' using ordinary query operations, without any support within [`pull`](../reference/main/xtql/queries.html#subqueries) itself.

## Bitemporality

It wouldn't be XTDB without bitemporality, of course - indeed, some may be wondering how I've gotten this far without mentioning it!

(I'll assume you're roughly familiar with bitemporality for this section.
If not, forgive me - we'll follow this up with more XTDB 2.x bitemporality content soon!)

- In XTDB 1.x, queries had to be 'point-in-time' - you had to pick a single valid/transaction time for the whole query.

    In XTQL, while there are sensible defaults set for the whole query,
    you can override this on a
    per-[`from`](../reference/main/xtql/queries.html#from) basis by
    wrapping the table name in a vector and providing temporal
    parameters:

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=bitemp-xtql-1,indent=0]

    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=bitemp-xtql-2,indent=0]
    ```

    ``` sql
    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=bitemp-sql-1,indent=0]

    Unresolved directive in introducing-xtql.adoc - include::../src/test/resources/docs/xtql_tutorial_examples.yaml[tags=bitemp-sql-2,indent=0]
    ```

    - You can also specify `(from <time>)`, `(to <time>)` or `(in <from-time> <to-time>)`, to give fine-grained, in-query control over the history returned for the given rows.
    - System time (formerly 'transaction time', renamed for consistency with SQL:2011) is filtered in the same map with `:for-system-time`.
- This means that you can (for example) query the same table at two points-in-time in the same query - 'who worked here in both 2018 and 2023':

    ``` clojure
    Unresolved directive in introducing-xtql.adoc - include::../src/test/clojure/xtdb/docs/xtql_walkthrough_test.clj[tags=bitemp-xtql-3,indent=0]
    ```

## For more information

Congratulations - this is the majority of the theory behind XTQL!
You now understand the fundamentals behind how to construct XTQL queries from its simple building blocks - from here, it's much more about incrementally learning what each individual operator does, and how to work with it via edn.

You can:

- check out the reference guides for XTQL [queries](/reference/main/xtql/queries) and [transactions](/reference/main/xtql/txs).

We're very much in **listening mode** right now - as a keen early adopter, we'd love to hear your first impressions, thoughts and opinions on where we're headed with XTQL.
Please do get in touch via the [usual channels](/intro/community.html#oss-community)!

[^1]: rows ...​ which themselves are otherwise known as 'maps', 'structs', 'records' or 'dictionaries' depending on your persuasion 😄
[^2]: although XTQL's `->` isn't technically macro-expanded - it's just data.
