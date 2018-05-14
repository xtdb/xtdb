## Internals

This document discusses and suggests different internal implementation
details and design.

### Indexing

#### Proposal A:

We have 5 indexes:

* keyword -> id
* content-hash -> doc
* eid/business-time/transact-time -> content-hash
* content-hash -> set of eids
* aid/value -> content-hash

Querying works by looking up the attribute and value in the aid/value
index, and resolving the resulting content hash into a set of entity
ids which have at any point in time had this value. These entities are
then resolved using the bitemporal coordinates using the
eid/business-time/transact-time index, and kept if the content hash is
the same.

Following references and joins both work by deserializing the current
node, and getting all the values for the attribute. Keywords are
resolved to their ids, and then the target content hashes are looked
up as in querying above.

The content-hash -> doc index is potentially a LRU cache backed by a
larger, full key value store shared between the query nodes.

#### Proposal B:

This is the initially implemented approach. This excludes
bitemporality for now; an imminent task is to modify this strategy to
accomodate it, or move towards another strategy.

* keyword -> attribute id (originally `frame-index-attribute-ident`)
* attribute id > keyword (originally `frame-index-aid`)
* content-hash/transact-time -> entity id (originally
  `frame-index-avt`)
* eid/attribute-id/transact-time -> attribute value (originally
  `frame-index-eat`).

Querying works by:

1) Looking up the attribute and value in the aid/value content-hash
index, and resolving the resulting content hash into a set of entity
ids which have at any point in time had this value.

2) These entity attribute values are then resolved again using the
eid/aid/transact-time index, and kept if the query predicate evaluates
to true.


### System of Record / Log

#### Proposal A: Immutable Log (Kafka)

The initial plan has been to store the transaction log in an immutable
log, such as Kafka. This is discussed in [retention](retention.md). In
both suggestion below compaction and deletion of data becomes
cumbersome, and might be easiest use in combination with separate
topics for different retention mechanisms or if we use encrypted
personal data and forget the keys.

The messages in this log can either be individual entities, grouped
into transactions, or transactions themselves. There are some pros and
cons of both approaches:

##### Proposal Aa: Messages are Entities

Pros:
 + Messages can have meaningful keys.
 + Can potentially use compaction.
 + Can potentially use Kafka transactions across topics.

Cons:
 + Harder to reason about transaction boundary, Kafka doesn't support
   transactional reads.
 + Cannot use LogAppend time in Kafka for transaction wall
   time, at least not without additional logic, related to above.

##### Proposal Ab: Messages are Transactions

Pros:
 + Clear transaction boundaries.
 + Can use LogAppend time for transaction wall time.

Cons:
 + Cannot compact topics, each key (if used) is an unique transaction
   id.
 + Business time is not necessarily the same for each entity in the
   message (not necessarily a problem, but can be confusing).

#### Proposal B: Distributed CRDT KV Store

This is easier to modify and delete data from, but requires much more
engineering and is a departure from the log based design. While Kafka
might still play a role, the system of record of CRDTs must likely
live somewhere else. Either directly in the nodes KV stores - which
makes the durability guarantees of them much higher - or in another
store.

As this can be a multi-master setup, it's likely to be more scalable
and it's also a bit more forward looking design than the idealisation
of an immutable log that has to be worked around.
