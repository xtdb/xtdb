## CRUX Implementation Notes

See the [MVP Summary](mvp.md) for the background and motivation behind
CRUX.

The document [Internals](internals.md) (and the other docs in this
folder) discusses our ideas *before* we wrote the MVP, while this
document aims to explain how it actually works *after* we wrote the
MVP. As can be expected, some early ideas live on.

### Introduction

This document mainly discusses the query engine, as this is the core
of CRUX and where most its complexity lies. There are other parts of
CRUX, like the ingestion piece, the KV store implementations, and the
REST API, but they are more straight forward to understand and
requires less theoretical background.

### Document Store

All documents in CRUX are consumed from the Kafka `doc-topic` and
stored on disk locally in an index:

+ `content-hash->doc-index` Main document store.

The documents must be EDN maps, and are serialised using Nippy. The
key is the SHA-1 of the serialised bytes. Every version of an entity
is a document.

The document store is accessed via an implementation of
`crux.db.Index`.

### Indexes

The attributes of each document is further indexed into two other
indexes:

+ `attribute+value+entity+content-hash-index` Secondary index of
  attribute values, mapped to their entities and versions (content
  hashes).
+ `attribute+entity+value+content-hash-index` Reverse of the above.

These are the main indexes used by the query engine. They are binary
indexes, there are two separate `crux.db.Index` implementations for
each index, combined via a binary index implementing
`crux.db.LayeredIndex`. This is important for joins, which we'll
return to.

`crux.db.Index` provides a single function, `seek-values` that takes a
key, and returns a tuple, where the first element is the byte array
representation (as via `crux.index.ValueToBytes) of the key, and the
second element a value, which can be anything. This byte array key is
what is used as the join key. If no value is found, `nil` is
returned. Identifiers, like entity ids and attributes, implement
`crux.index.IdToBytes` and are always represented as a SHA-1 hash.

Note that while at the lowest level. indexes are backed by the KV
store directly, but at the higher levels they are composed and
delegate to each other, this is referred to virtual indexes. Even at
the low levels the key and values returned aren't a one-to-one mapping
to what is actually stored on disk. And even at the high levels the
keys are always a byte array. This allows indexes at different levels
of abstraction to be joined with each other.

A layered index is used in the main join in in place of its child
indexes, presenting each of the child indexes in total join order. As
the join proceeds down the tree, `open-level` and `close-level` will
be called, telling the binary index which index to use at the current
depth. The use of the AVE or AEV index depends on which is applicable
in the total join order. More about this in the Query section below.

These indexes (as do most) also implement `crux.db.OrderedIndex` that
allow next operations. Both `seek-values and `next-values` return
`nil` if nothing. In an ordered index, `seek-values` will potentially
return the next value after the key if the key is not found
directly. An ordered index must return all values for a key in a
single call, that is, duplicate keys are not allowed, as this breaks
the underlying unary join algorithm, see Query below.

Finally, the transaction operations are indexed into:

+ `entity+bt+tt+tx-id->content-hash-index` Main temporal index, used
  to find the content hash of a specific entity version.

The latter used to filter the values from the attribute indexes to
only include versions valid at the business and transaction time used
during a query.

As indexes can be composed, they can also be decorated. This is how
range queries work, which simply constrain and skip part of the key
range of an underlying index. An index can be decorated with more than
one range constraint.

### Query

The entire query is represented as a n-ary join layered index, where
each level represents a variable in the join, and the levels the join
order, where the root is the first variable in the join order etc.

At each level there is a unary join index, consisting of all indexes
representing each occurrence of the variable. As mentioned above, the
same index might be part of more than one unary join, each time
representing a new variable at a new level. The important thing is
that all variables across all indexes participating in the join share
the same total variable order.

Arguments and constants are represented by a special in-memory
relation index, and participate in the join like any other index. Each
constant has its own single element index, while the arguments
potentially have several elements.

Apart from range constraints affecting a single index, there's also a
constraining decorator that allows the walk of the tree to be
constrained at different depths. This is how predicates, the
unification operators and sub queries like `or` and `not` are
implemented.

As mentioned above, as the tree is walked, a result map is being built
up with a key for each variable that gets bound. As these variables
become available, constraints can fire. For example, a predicate will
fire when the arguments have been bound, and potentially short circuit
the walk by returning `nil`. `not` is a special kind of predicate that
issues a sub query and removes all results from that query in the
parent result. Note that all this happens many times during the query,
as the tree will be traversed up and down, each time binding new
values to the variables.

Predicates that return values, and invocations to `or` and `or-join`
(which is also how rules are represented) will potentially bind new
values in the sub tree that is being walked. This is dealt with by a
place holder relation that gets its values updated by the result of
the sub queries, and then participate in the join like any other
relation (like a constant or the arguments).

Unification differs from predicates mainly by not operating on the
result map, but on the byte array keys. This avoids looking up the
actual values.

The elements in the result map will either be literal values, like for
arguments or constants, or sets of entities owning the value for a
variable. As each variable will have an attribute (with the entity
position defaulting to `:crux.db/id`), resolving the actual value for
an entity in the result set is done by looking up its document and
then the attribute in it. As an attribute might be many valued, the
value is filtered against the actual key as byte arrays to find the
value that we're actually looking for at this point in the join.

Once the n-ary join is setup, the layered index is simply walked as a
tree, and the cartesian product of the results is presented as the
result to the query. This is also true for sub queries, in which case
the results are used in the constraint in different ways.

### Rules

Rules are expanded to `or-join` expressions, which as mentioned above,
are implemented as sub queries. A combination of expanding the rules,
renaming its local variables, and tabling to avoid infinite recursion
is used during execution.
