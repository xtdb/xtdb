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
