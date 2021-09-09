# 8. Datalog queries

Date: 2021-09-09

## Status

Proposed

## Context

XTDB should support graph queries via Datalog.

## Decision

We will support (a subset) of [EDN
Datalog](https://docs.datomic.com/on-prem/query/query.html#query).

There will be a Clojure API and we want to maintain a reasonable level
of [compatibility with classic](0003-backwards-compatibility.md)

## Consequences

All processing needs to map down to the internal [data
model](0002-data-model.md). This needs to be balanced with classic
features and [SQL](0007-sql-queries.md).

As the internal data model stores lists and doesn't flatten
cardinality-many values, we will need an unnest operator to do this if
we want EDN Datalog (SPARQL) behaviour.
