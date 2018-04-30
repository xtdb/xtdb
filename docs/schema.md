## Schema

**Note: in
[RDF](https://en.wikipedia.org/wiki/Resource_Description_Framework), a
triplet is made up of Subject Predicate Object, in Datomic the same
thing is called Entity Attribute Value.**

### Triplets vs Documents

Crux will deal with entities as either a triplets or documents. The
identifier in the subject position in the triplet represents its
entity. In a document store, each document would be represented as
versions of this same entity. In a triplet store the document is
implicit, being all predicates reachable from an entity.

The triplets, or the properties of the documents, can be connected to
other entities in a graph. For more about this, see
[query](query.md).

One reason to use documents and not triplets, is that triplets are
really an implementation detail, and users are likely to think about
distinct versions of entities, like when using the map form in
Datomic. When allowing access to the triplets themselves, the user can
create a new version of an entity "by mistake" without seeing the full
state of the resulting entity.

The triplets will further need at least bitemporal query support, see
[bitemp](bitemp.md) and the ability to build various forms of
retention and provenance models on top of the raw data, see
[retention](retention.md).

**Triplets are the most expressive model, but documents easier to
reason about for the user.**

**Note: we're not talking about large binary documents here, just
different ways of reasoning about entities.**

+ [RStore: A Distributed Multi-version Document
  Store](https://arxiv.org/abs/1802.07693)

### Scalar Types and Indexing

The scalar types can be assumed to be normal values or references to
other entities. References to entities outside the store could also be
considered. In RDF, references are always some form of URI.

The data model can in the simplest case treat everything as bytes, and
not have any specific knowledge of different data types, and index all
attributes in a
[Hexastore](https://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore)
style. More likely the schema will know at least of the URI or
references to other entities, as indexing all 6 permutations of each
triplet takes up lot of space.

The scalar data types can further be modelled closely on Datomic and
the EDN data model, or say XML Schema to stay closer to RDF. Most
likely we want support for range queries across types supporting it.

Most likely we want to keep the core data binary and the way its dealt
with and indexed handled by an extensible interface so the user can
add their own types.

### Non-scalar Types

In Datomic one can have an attribute with cardinality many, which
effectively makes it behave like a set, allowing multiple triplets at
the same entity/attribute. This is problematic for various
reasons. For example, you need to explicitly remove all elements of
the set if you want to replace it. Another issue is that you cannot
easily support order to model lists. In triple stores there are ways
to work around this problem, but usually not in an elegant way.

In a document store, simple container data types, like lists, sets,
maps, and even nested component types, can be more easier defined and
reasoned about. A new version of a document always represent the
entire state. The underlying implementation can then choose to use
deltas or triplets to model this.

Another issue with predicates that have multiple values, is that it
complicates indexing (and hence queries) in a bitemporal setting, as
one need to ensure that source and target of a reference are visible
at the same time. This can be mitigated by explicitly adding explicit
retractions to the index but this can double the index size, see
[bitemp](bitemp.md) for more about this.

### CRDTs

One option is to allow the underlying store directly support
[conflict-free replicated data
types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
to allow scaling without locks, while still provide strong eventual
consistency using a model that is possible to reason about predictably
for the user. This support would be built into the types of the schema
itself. See [transactions](transactions.md) for more about this.

+ [Coordination Avoidance in Database
  Systems](https://arxiv.org/abs/1402.2237)

### Additive Schema

One option, is to like MongoDB or Elasticsearch, allow usage without
having to explicitly define the schema upfront. In Mongo one still
needs to specify which fields needs to be indexed and how. And in
Elasticsearch, one also need to define the schema for anything but
trivial usage. In both cases you can still have parts of your
documents that are undefined from a query POV, and simply "there".

Datomic requires a stricter schema, as does RDF, as any entity
referred to, predicates in RDF are also themselves entities, similar
to how in Datomic the attribute is defined in the schema.

We have to assume that data evolves fast, and that it will be hard to
properly predict all data that can be written into the store, and all
the versions of the schema. Hence, having a defined schema complicates
matters, as this itself then also needs to evolve, vast data migrated
etc. For these reasons, one needs either a write once, additive only,
schema, or make the schema implicit.

### Implicit Indexes

The schema exists for two different reasons, to allow the user reason
about the data, and ensure it adheres to the type system, but also
simply to help the query engine index things intelligently.

The indexes themselves are implicit in the queries done, but one
cannot wait until the last minute to index vast quantities of data, so
the easiest middle ground is to keep the schema at a minimum, but be
explicit about what is indexed and how. This can be done on value type
level, and not necessarily by defining an index for each
predicate. That is, certain types automatically gets indexed, but we
don't care about what name they have.
