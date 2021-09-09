# 3. Backwards compatibility

Date: 2021-09-09

## Status

Proposed

## Context

XTDB needs some level of compatibility with classic. At the minimum we
need to import the data and timeline.

### Transaction processing (write side)

#### Rollback

Currently c2 takes a zero-copy of the live data and present that for
queries. The easiest way to do rollbacks is to do this the other way
around. Have the transaction take a zero-copy slice, modify it (append
only) and then if it commits, replace and release (decrease the
reference count) of the previous watermark.

The temporal in-memory index is a persistent data structure, so you
wouldn't need to do anything here, just go back to the previous
version.

#### Transaction functions

Nothing really changes here actually, once everything else works.

#### Data model

We would move towards c2's Arrow based data model. I would suggest we
do a few additions:

#### match/cas

Can be implemented as a scan of the same columns. For consistency, its
easiest to write the expected doc to Arrow as well and just compare
the rows.

#### Eviction

Doesn't exist in c2 yet, but is somewhat orthogonal. Can be built in
various ways. A simple design I think may work is:

1. hard delete all overlap in the temporal index.
2. write row-id sets into the object store as a work items, named by
   the evict-ops row id.
3. out-of-band, pick all work-items you can find for a chunk, download
   it, copy it, skipping the row ids in the set.
4. upload it in place, breaking "immutable chunks" principle.
5. sanity check that the uploaded file is the one you expect, then
   delete the work items you processed, as this is done after, if
   someone else uploads before you, they would have seen the same or
   later work items. Might require some tweaks to actually be safe,
   but possible to design a protocol. The S3 database paper has one.
6. local caches will still contain the data, but it will be filtered
   out by the temporal index, let it disappear naturally from the
   nodes. If forced, cycle the nodes regularly to ensure this happens
   within regulatory time frames.

Note that eviction in c2 only deals with the object store, not the
transaction log, so if the log has infinite retention, one cannot
guarantee data being removed there. If this is a deal-breaker, we
could redesign the document topic with slightly different constraints
than now, but reopens many issues with eviction and complexity.

#### Speculative transactions

The easiest way to do this is c2 is to take defensive (or
copy-on-write) copies of the live roots in the watermark (a watermark
is kind of like a db in c2). This is a bit slow, but fundamentally
doable. Note that all data won't be needed to be copied, just the
current slice and the current in-memory temporal index.

#### Lucene

A c2-native solution would be to have secondary indexes
participate in finish chunk, and maybe also a collective LSM-style
merge process. Temporal could maybe also use this capability if it
existed.

#### History API

If we want to keep supporting it until we have full temporal support
(including transaction time), we can simulate it via the scan operator
I think.

#### Pull

Would need to be partly rewritten unless I'm mistaken, but isn't
fundamentally hard, especially not now when the Pull engine is based
on the index store, it would be executed as recursive scan calls.

#### Clojure predicates

The direction in c2 is to not support arbitrary Clojure
predicates. For performance reasons we should still implement typed,
fast predicates for the "supported" language. That said, we can also
support a :default in the multi-method which unwraps any special
representation (like a Date being a long internally) and invoke a
normal Clojure function, and then coerce the result back. Doesn't
exist but not hard to build.

#### Relation, collection and tuple bindings

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

#### Multiple and external data sources

The c2 logical plan and Datalog supports multiple data sources. We can
also relatively easily support other Arrow sources of data, like CSV,
Avro, JDBC, JSON, Parquet etc. which would bring features classic
currently doesn't have. There's a CSV spike, but area requires more
work, but with good cost / benefit, and can be farmed out to someone
else.

#### Advanced Datalog

- `or` unions.
- `or-join` semi-joins against unions.
- `not`, `not-join` anti-joins.

Note that we don't support more than one variable in joins, so we
either need to fix that in c2, or combine the joins with selects to do
further filtering.

#### Rules

Some easier rules can be compiled to our fixpoint operator in c2, more
advanced rules quite hard to do in the generic case. There's a
potential direction where you create ad-hoc operators for them
somehow. To be explored. Quite chunky piece of work risk-reward-wise.

#### Calcite and other modules compiling to Datalog

These may "just work", or if they rely on complicated parts of the
classic engine, require rework.

## Decision

TBD

## Consequences

TBD
