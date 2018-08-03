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
directly tied to a specific type.

History could be made easier to work with. While supporting full on
history queries is hard, accessing and doing some form of filtering of
the time line could be made easier.

Using CAS could be made more intuitive, having some form of mechanism
to allow someone to easy check if their operation passed or not. In
this area, we could also explicit introduce support for generic
transaction functions. There's limited support to add new transaction
commands at the moment, but this should not be necessary, instead a
`:crux.tx/fn` command could be introduced.

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
+ Revisit storage layer, explore succinct data structures and other
  approaches than B-trees.
+ Revisit storing data straight into the indexes, avoiding looking up
  the docs. Will decouple constraints depending on binary indexes,
  which currently have to wait until both variables are bound.
+ Revisit doc store, and the fact that `:crux.db/id` currently has to
  be stored inside the docs, removing ability for cross-entity
  structural sharing.
+ Revisit id generation and approaches where structured or similarity
  between ids can be utilised.
+ Explore bloom filters and similar caches.
+ Better query planner, at the moment a join order is picked once and
  never revisited.
+ Ability to collect statistics and data frequencies and feed them
  into the query planner to help it choose join order.
+ Support variables in attribute position. Will likely require more
  indexes.
+ Support full nested expressions apart from simple predicates.
+ Support triple patterns in map form, when the e is shared among
  several patterns.
+ One could aim for closer Datomic/Datascript compatibility,
  introducing more of their functions and support bindings and `:in`
  etc.
+ We want to add performance test suites which also compare CRUX
  against other stores on the same queries. This would preferably be
  running in the cloud and generate reports daily or so.

#### Enhanced RDF and SPARQL Support

Currently we support a subset of SPARQL, not always with the correct
semantics, and we can parse RDF but not necessarily in a consistent or
strict way. There's no way of getting "raw" RDF back out, it's all
EDN. We also don't support any OWL reasoning or RDF schema
resolution. The latter is somewhat hampered by the fact that we don't
support variables in attribute position.

One could also aim to map and support the entire set of standard
SPARQL functions to Clojure implementations.

This area is quite time consuming, and will require work in the core
query engine to support certain things, but a lot of it can happen at
its own layer. A positive is that there's a set of specifications to
follow.

#### Subscriptions

Subscriptions can be done in several ways, from the trivial (but less
likely very useful) to very sophisticated where the core query engine
becomes totally tuned for the incremental case and turns more into a
real Datalog rule engine. This is a large and complicated area and one
needs to strike the right balance, especially if we're talking Phase
2. Subscriptions become even more complex in the face of deletions.

In the simplest case, one can fire all subscribed queries after each
transaction, potentially filter the result tuples to ensure that at
least one value in them has changed, and then somehow notify or send
this result to the subscriber.

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
to the same query, the query should only need to fire once, even if it
is sent to several destinations.

There needs to be a lot more analysis here even in this simple case.

The more advanced case introduces proper incremental view maintenance
and move CRUX more into rule engine territory, where subscriptions
could be rules, which would fire when new matches become
available. Like above, this gets complicated by the fact that rules
might run at different nodes but submit data back into the same
Kafka. We neither want all rules to fire at all nodes, but we also
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

This entire area needs much further analysis. But the area has the
benefit of not being so dependent on the query engine (though changes
to keys would affect both) and can be worked on as a separate
stream. Writes to the index during a roll-up window would need to
overwrite the current version.

#### Provenance

We want to be able to record where data came from, and likely also
issue queries about this. This is most easily done by adding "system"
entities on write in various ways, connected to the transacted
entities, but which can still be queried using regular queries.

If we support transaction functions or rule engine features generating
new data, this would also need to be tracked. In the extreme case, one
should then be able to see the entire chain of data (and rules) that
lead to the creation of a specific entity version.

#### Authorisation on Read

Related to provenance, different data types, and different entities,
might have restrictions on who can see them. Sometimes one might be
allowed to join on the data but not read it, sometimes it might be
filtered away totally from the user. As this is a per-user filter, it
might be tightly integrated in the query engine. A slightly easier
case is to add it as a post processing layer, removing tuples
containing entities or data the user is not allowed to see.

#### Sharding

At the moment we assume that all data can fit in a single `tx-topic`
and be indexed on each individual query node. This assumption will
likely not scale. Sharding is on one level easy, you simply run
several CRUX instances, or support several separate partitions or
topics in a single CRUX instance, allowing query nodes to subscribe to
a subset of these.

The hard part is cross-shard queries, as this will require both
coordination and operation across nodes to generate the result. This
would break several assumptions currently made by the query engine. A
work around can be found at firing queries to several nodes and then
manually filter and joining the result in the code, but this would be
outside CRUX.
