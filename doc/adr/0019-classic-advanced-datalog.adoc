# 19 Classic Advanced Datalog

Date: 2021-09-14

## Status

Proposed

## Context

- `or` unions.
- `or-join` semi-joins against unions.
- `not`, `not-join` anti-joins.

Note that we don't support more than one variable in joins, so we
either need to fix that in c2, or combine the joins with selects to do
further filtering.

### Relation, collection and tuple bindings

The c2 logical plan supports single value projections only. There are
a few routes here, we can support more advanced projections in the
projection operator directly, or we can support binding
list-of-structs (everything can be represented as a relation binding)
directly as a result, and then introduce a second unwrap operator
(there are various names for this in relational algebra, to be
decided) which is a bit like flatmap and would flatten a specfic
nested value. A simpler half-way house is to support list of scalars
only and unwrap that. Datalog would compile these bindings to a
combination of project/unwrap.

### Calcite and other modules compiling to Datalog

These may "just work", or if they rely on complicated parts of the
classic engine, require rework.

## Consequences
