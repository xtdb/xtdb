## Phase 2

After the [MVP](mvp.md) there are several directions we can take CRUX.
Phase 2 is assumed to be 3 months long, roughly during the end of 2018
and early 2019. Staffing is yet to be decided.

One goal of Phase 2 is to reach a point where CRUX can be shared with
a wider audience, potentially even made public. To facilitate this,
proving and enhancing the MVP in real use cases will likely be the
most important thing.

On the feature side, there are several avenues we can pursue:

+ Enhanced and extended API.
+ Operations and monitoring.
+ Query engine performance and enhancements.
+ Enhanced RDF and SPARQL support.
+ Subscriptions.
+ Retention, TTL, roll-ups.
+ Provenance.
+ Authorisation on read.
+ Sharding.

Realistically, we won't have time for all of these. They are listed in
roughly the order in which they are naturally to incrementally evolve
based on where the MVP currently stands. That said, we can choose to
prioritise newer areas like subscriptions or retention.

These points are discussed below.

**It's worth keeping the "unbundled" concept in mind when reading this
and reflect on what can be built as layers or on the side of CRUX, and
how to avoid building it all into the core in an interlocking way.**

### Features

#### Enhanced and Extended API

At the moment CRUX provides a quite low-level API and we could expose
a richer, more user friendly set of functions on top of this. This
includes things like making it easier to get the actual docs for a
query, currently they sit in the meta data of each tuple.

We could also make it easier to extend indexing, adding new value
types and introduce a protocol explicitly for optional indexing. One
can currently choose to not index a value, but have no access to the
document at this point. Also, the indexing predicates might not be
directly tied to a specific value type.

History could be made easier to work with. While supporting full on
history queries is hard, accessing and doing some form of filtering of
the time line could be made easier.

Using CAS could be made more intuitive, having some form of mechanism
to allow the user to easily check if their operation passed or not. In
this area, we could also explicitly introduce support for user defined
transaction functions. There's limited support to add new transaction
commands at the moment, but this should not be necessary, instead a
`:crux.tx/fn` command could be introduced.

Evolving the API is best done in combination with actual use of CRUX.

#### Operations and Monitoring

We have a start of an AWS deployment stack for CRUX, and a simple
status API. There is an immense amount of work that could be done in
this area to make CRUX easier to run and give insights while doing so,
via dashboards, better logging and potentially query consoles.

#### Query Engine Performance and Enhancements

There's a lot of improvement to be made on the query engine internals,
even without adding new features to it. Among the things are:

+ Utilise index sort order to avoid sort on order by.
+ Embed sub queries as nested n-ary or-joins where applicable.
+ Revisit storage layer, explore succinct data structures, sparse
  matrices and other approaches than B-trees.
+ Revisit storing data straight into the indexes, avoiding looking up
  the docs. Will decouple constraints depending on binary indexes,
  which currently have to wait until both variables are bound. Using
  covering indexes is also potentially faster.
+ Revisit doc store, and the fact that `:crux.db/id` currently has to
  be stored inside the docs, removing ability for cross-entity
  structural sharing.
+ Revisit SHA-1 id generation and approaches where structure or
  similarity between ids can be utilised.
+ Explore bloom filters and similar caches to avoid IO.
+ Better query planner, at the moment a join order is picked once and
  never revisited.
+ Ability to collect statistics and data frequencies and feed them
  into the query planner to help it choose join order.
+ Support aggregates. This needs exploring, as some aggregates might
  not be possible to do by simply operating on the lazy result. If
  nothing else, we might want to supply aggregate functions that helps
  the user to do so.
+ Support variables in attribute position. Will likely require more
  indexes.
+ Support full nested expressions apart from simple predicates.
+ Support triple patterns in map form, when the `e` is shared among
  several patterns.
+ One could aim for closer Datomic/Datascript compatibility,
  introducing more of their functions and support bindings and `:in`
  etc.
+ We want to add performance test suites which also compare CRUX
  against other stores on the same queries and data. This would
  preferably be running in the cloud and generate reports daily or
  so. We also want to test on larger data sets than the small ones
  we've used so far.

#### Enhanced RDF and SPARQL Support

Currently we support a subset of SPARQL, not always with the correct
semantics, and we can parse RDF but not necessarily in a consistent or
strict way. There's no way of getting "raw" RDF back out, it's all
EDN. We also don't support any OWL reasoning or RDF schema
resolution. This is somewhat hampered by the fact that we don't
support variables in attribute position.

One could also aim to map and support the entire set of standard
SPARQL functions to Clojure implementations.

This area is quite time consuming, and will require work in the core
query engine to support certain things, but a lot of it can happen
independently in its own layer. A positive is that there's a set of
specifications to follow.

#### Subscriptions

Subscriptions can be done in several ways, from the trivial (but less
likely very useful) to very sophisticated where the core query engine
becomes totally tuned for the incremental case and turns more into a
real Datalog rule engine. This is a large and complicated area and one
needs to strike the right balance, especially if we're talking about
things doable during Phase 2. Subscriptions become even more complex
in the face of deletions.

In the simplest case, one can fire all subscribed queries after each
transaction, potentially filter the result tuples to ensure that at
least one value in them has changed since last time the query ran, and
then somehow notify or send this result to the subscriber.

Simple improvements to the above scheme can be to detect if any of the
new documents transacted actually could affect the query result or not
before issuing the query. At the minimum, this means that the document
needs to contain an attribute referred to in the query.

Results of subscriptions can be sent either locally, assuming library
usage, where one registers the subscriptions directly in code running
on the query node itself, or one can imagine a scheme where users
registers subscriptions over Kafka in various ways. There are several
issues that needs to be solved here. Preferably only one query node
should send replies, this can potentially be done by partitioning the
subscription topic(s) on user/query. As several users might subscribe
to the same query, the query should only need to fire once, even if
the result is sent to several destinations.

There needs to be a lot more analysis here even in this simple
case. By their nature, subscriptions happen on newly transacted data,
and never on history, but there's a question about what should happen
when someone writes into the past.

The more advanced case introduces proper incremental view maintenance
and move CRUX more into rule engine territory, where subscriptions
could be rules, which would fire when new matches become
available. Like above, this gets complicated by the fact that rules
might run at different nodes but submit data back into the same
Kafka. We neither want all rules to fire at all nodes, and we also
don't want the rules to fire in an uncoordinated way, potentially
generating invalid data due to race conditions.

#### Retention, TTL, Roll-ups

CRUX currently has support to evict documents. This could be extended
in various ways with TTL of documents, or having documents being
mutable in a time window, say during this hour all transactions write
to the same key for an entity, regardless content hash, but during
next hour they write to a new key, this allows Kafka to compact away
versions and save space. Exactly what the key would be here is a bit
unclear, it would need to be based on time stamp instead of content
somehow.

Implementing TTL could be hard, Kafka has support for it, but on a
topic level, one potential approach is to have more than on
`doc-topic` with different TTL settings. Another approach is to have
documents which have reached their TTL be evicted during transaction
processing.

This entire area needs further analysis. But the area has the benefit
of not being so dependent on the query engine (though changes to keys
would affect both) and can be worked on as a separate stream. Writes
to the index during a roll-up window would need to overwrite the
current version.

#### Provenance

We want to be able to record where data came from, and likely also
issue queries about this. This is most easily done by adding system
entities on write in various ways, connected to the transacted
entities, but which can still be queried using regular queries. In
theory this can be done totally on top of the current API, as the
provenance meta data is just other entities.

It also leads to the question if we should or need to provide this
directly as a feature, or just have the ability to do so? It can be as
simple as adding a provenance document to the transaction with meta
data about the other documents in the transaction. If necessary, help
with this could be provided in a higher level API.

If we support transaction functions or rule engine features generating
    new data, this would also need to be tracked. In the extreme case, one
should then be able to see the entire chain of data (and rules) that
lead to the creation of a specific entity version.

There's a w3c standard for provenance which may or may not be helpful.

#### Authorisation on Read

Related to provenance. Different data types and different entities
might have restrictions on who can see them. Sometimes one might be
allowed to join on the data but not read it, sometimes it might be
filtered away totally from the user. As this is a per-user filter, it
might be tightly integrated in the query engine. A slightly easier
case is to add it as a post processing layer, removing tuples
containing entities or data the user is not allowed to see.

Deciding what a user can see might itself require a query, as this
information, like provenance could be stored as system
entities. Unlike provenance the query engine would need some form of
knowledge or support for this, potentially simply by a generic way of
filtering the results which the user cannot bypass.

"Real" authorisation for local clients would be hard to achieve, as
they can always read the local index, so that would require encrypted
indexes. In this context, we're talking about authorisation in the
sense of help restricting the data sent from the node to the client,
not hiding it from the code running on the node.

#### Sharding

At the moment we assume that all data can fit in a single `tx-topic`
and be indexed on each individual query node. This assumption will
likely not scale. Sharding is on one level easy, you simply run
several CRUX instances with their own Kafka topics, or support several
separate partitions or topics in a single CRUX instance, allowing
query nodes to subscribe to a subset of these.

The hard part is cross-shard queries, as this will require both
coordination and co-operation across nodes to generate the
result. This would break several assumptions currently made by the
query engine. A work around can be found at firing queries to several
nodes and then manually filter and joining the result in the code, but
this would be outside CRUX.

As the query engine is built on the concept of composing indexes and
uses Query-Subquery, supporting remote queries inside a query should
be possible. But there needs to be a way of deciding when to go to
another node for parts of the result, which will be related to the
sharding strategy in the first place. As the time line is only valid
within a single `tx-topic` or shard, coordination between shards needs
to happen using some other mechanism.

In the simplest case it can be to read the latest known data, but
there are obvious issues with this. Another approach is to require a
consistent business time definition across shards, and always issue
queries with a business time. While this is easy to reason about, it's
hard to ensure and achieve for all the normal reasons when it comes to
make guarantees about time in a distributed system.
