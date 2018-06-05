## Internals

This document discusses and suggests different internal implementation
details and design.

### Indexing

#### Proposal A:

We have 5 indexes, spread across a few logical stores:

*Document Store*

* content-hash -> doc
* aid/value -> content-hash

*Transaction Log*

* eid/business-time/transact-time/tx-id -> content-hash
* content-hash -> set of eids

*Internals*

* keyword -> id

External ids are assumed to be keywords (idents). This mapping of
keywords to ids is an optimisation, and could be replaced by MD5 or
SHA1. This is used for both eid and aid.

Querying works by looking up the attribute and value in the aid/value
index, and resolving the resulting content hash into a set of entity
ids which have at any point in time had this value. These entities are
then resolved using the bitemporal coordinates using the
eid/business-time/transact-time/tx-id index, and kept if the content
hash is the same.

Following references and joins both work by deserializing the current
node, and getting all the values for the attribute. Keywords are
resolved to their ids, and then the target content hashes are looked
up as in querying above.

Many valued attributes (cardinality many) are simply repeated in the
aid/value index and taken from the document on initial indexing,
there's no special handling of them. Order in lists etc. are preserved
by the original document.

Range queries are served by directly scanning the aid/value index, as
it does not contain any information about time, and will be sorted by
order. The values themselves needs to be encoded in a binary format
that preserves order.

The content-hash -> doc index is potentially a LRU cache backed by a
larger, full key value store shared between the query nodes.

When looking up using transact-time, business-time defaults to
transact-time. This will filter out eventual writes done into the
future business-time, which would need to be found via an explicit
business-time/transact-time pair. The transact-time might not be an
actual date, but could be a monotonic transaction id or maybe some
form of causal context. If the transact-time is a date, it should be
taken in an consistent (monotonic) manner, and should not be assigned
by the client.

The tx-id is based on the offset of the transaction in the transaction
log. Both this tx-id, the transact-time and business-time are possible
to retrieve from the main eid/business-time/transact-time/tx-id index
without storing them inside the entity itself.

It's worth noting that the content-hash -> doc and aid/value ->
content-hash indexes form their own key value store with secondary
indexes. The content-hash -> doc could be represented as a compacted
topic in Kafka, which could be spread on many partitions and topics
(for various types of data and retention). In this case the
transactions themselves could simply contain
eid/business-time/transact-time/tx-id -> content-hash or variants
there of, pointing to the key value topic and is kept smaller. A
content hash of nil would be a retraction of the full entity. CAS can
also be implemented via supplying two content hashes. Upserts are
slightly trickier in this scenario, as this would depend on Crux
merging the documents and generate a new content hash this could
happen on the client side in conjunction with CAS, so that one knows
that one updates the expected version.

This way the values in the key value topic can be purged (evicted)
independently of rewriting the main log by overwriting them with
either nil or in a more advanced usage, a scrubbed version of the data
(this has a drawback of breaking the hash, so would need some meta
data).

For full erasure, the key value store also need to keep track of the
keys a content-hash have in the aid/value index, as these need to be
purged as well, but the set can conceivably change if the indexing
code changes. This is likely better dealt with via index migrations,
where the simplest case is to simply retire nodes using an old index
formats, and hence deleting their indexes.

##### Transaction Log Format

The transactions could look something like this, the time is business
time:

```clj
[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
"090622a35d4b579d2fcfebf823821298711d3867"
#inst "2018-05-18T09:20:27.966-00:00"]

[:crux.tx/cas :http://dbpedia.org/resource/Pablo_Picasso
"090622a35d4b579d2fcfebf823821298711d3867"
"048ebba27e1da223ce97dded59d46e069ddf921b"
#inst "2018-05-18T09:21:31.846-00:00"]

[:crux.tx/delete :http://dbpedia.org/resource/Pablo_Picasso
#inst "2018-05-18T09:21:52.151-00:00"]
```

In this model, upserts or merging can be implemented via CAS on the
client, first fetching the document then merging it. It could
potentially be done by Crux itself, but it complicates the ingestion,
as a new resulting document would be generated and indexed.

The key itself could also be hashed if necessary if we wish to hide it
from the transaction log, in which case you would have:

```clj
[:crux.tx/put "9049023351ca330419c5c5072948a305343c8d91"
"090622a35d4b579d2fcfebf823821298711d3867"
#inst "2018-05-18T09:20:27.966-00:00"]
```

Where 9049023351ca330419c5c5072948a305343c8d91 is the SHA1 of
the :http://dbpedia.org/resource/Pablo_Picasso keyword.

Omitting business time defaults it to the transaction time, which is
always taken from the message itself:

```clj
[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
"090622a35d4b579d2fcfebf823821298711d3867"]
```

While the business time often might stored as a field inside the
entity itself, this is not always the case and not mandatory, and the
content hash would change simply by updating a document even without
any other "real" changes. This information is stored in the
eid/business-time/transact-time/tx-id index key so it can be accessed
regardless, and entities could optionally be enriched with this meta
data on read, see above.

#### Proposal B:

This is the initially implemented approach.

* ident -> ident-id
* ident-id > ident
* AV: attribute-id/content -> entity-id
* EAT: entity-id/attribute-id/business-time/transact-time -> attribute value

Note that the current implementation differs slightly from the above:

* AV contains business time, which should be considered to remove
* AV also contains entity ID, which may stay, so that the look-up of
  the value is not required.

Querying works by:

1) Looking up the attribute and value in the AV indes, and resolving
the resulting content hash into a set of entity ids which have at any
point in time had this value.

2) These entity attribute values are then resolved again using the
eid/aid/business/time/transact-time index, and kept if the query predicate evaluates
to true.

Pros:

* Less storage needed compared to proposal A - only datoms that change
  need to be stored.
* Less deserialisation needed of data (typically)

Cons:

* More jumps are needed. In A you always have the full document on
  hand, where-as in B you need to look-up each value that is needed.
* Proposal A ships a document store as part of the implementation. B
  will need a document store if all the data pertaining to an entity
  isn't going to be indexed (this leads on to proposal B2). This
  disadvantage of a lack of functionality could be seen as an
  advantage with a nod towards the 'unbundled' nature of Crux - i.e. B
  does one thing and does it (hopefully) well.

Further analysis/work needed:

* How do we compact/evict stale data from the AV index?
* Work is needed to bring B up to compatibility status with
  Datasource.

#### Proposal B2

An iteration of proposal B, where the 'doc store' is split out from
the indexing layer.

The rationale for this iteration:

1) The actual content (docs) can fed into the system via a different
ingestion topic, that can then be compacted, for example to remove
duplicate docs (docs are keyed by content hash).

2) Transactions are necessary to keep immutable, which would reside on
their own topic / indices. The content topic can otherwise then be
subject to excision, which is necessary for offering data retention
strategies (for compliance, i.e. GDPR).

3) It's desirable to keep documents in Crux in a way that doesn't
necessarily subject them to a full index (triplet style), and then
make them relatively expensive to stitch back together.

The initial strategy for B2 is have the indexer lag behind the
transaction log. The indexer will only index the data presiding from a
transaction when the corresponding document is available in the
doc-store.

This does present some difficulties, i.e. a user might store a
transaction, but this no longer means it is actually indexed.

An advantage of this approach is that the doc-store should be
pluggable - users might want to store their own documents in S3 or
some other store. In this case there is further analysis on where a
caching layer would belong, that would be necessary for performance.

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

### Identifiers

#### Proposal A: Use external IDs

It's up to the users to supply their own IDs, such as UUIDs.

The advantage of this approach are:

1) Users get to use their own (upstream) IDs, which is more
sympathetic to the enterprise reality of multiple data-stores, and for
when users are working with external data-sets that already come with
IDs.

2) External IDs are needed anyway, if data is to be sharded across
nodes and needs to be reconciled in some way.

3) No-need for temp IDs, thus simplicity of operation.

4) It aligns with the intuitions of Crux being an 'unbundled' DB; the
ID generation management is another piece that is unbundled, given
over to the user's control.

The downside is that external IDs will not be optimised for internal
usage, i.e being numeric IDs to be used directly as part of Crux's
indices. Therefore IDs may will to be mapped to/from accordingly when
data goes in and out.

This could be mitigated by using MD5s rather than numerical IDs
internally (albeit at a higher cost), but a mapping will still to be
made to reconstruct the external IDs when returning data.

#### Proposal B: Use Crux IDs.

Crux assigns and returns it's own IDs.

The advantage of this approach is that the same constant IDs are used
throughout all interactions with Crux, meaning there is no need for
mapping or hashing, which may hamper performance, which may then
elicit the need for a cache of sorts to mitigate.

In this approach we would need to use the idea of temp-IDs to help
users insert data for two reasons:

1) Users can get a handle on the data they are inserting, for later use.

2) We may insert data that refers to a part of itself; a temporary ID
will be needed to join the data inside of the transaction together.

#### A vs B

There are pros and cons of both approaches. It might be possible to
offset the performance implications of A, and the current rationale is
that we should prioritise superior design and user experience over
prematurely worrying about performance.

Allowing users to work with their own IDs is a desirable feature,
helping users to import data and allowing them to not worry about
managing multiple IDs across their systems and datasets.

If we choose A first, we can always fall back to B.

### Eviction

#### Proposal A: Evict Content History of Entity

In the simplest case, we can evict the content of the history of an
entity in the log before the transaction time:

```clj
[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso]
```

The indexing node will need to find all versions of the entity before
this time, and get rid of them. In Proposal A for Indexing above, the
documents must then be deleted from a compacted Kafka topic. To make
the decision one needs to have consumed the transaction log up until
the eviction message is read so one can decide which hashes to delete.

The easiest way to achieve this is to have the indexing nodes all send
deletion messages to the compacted document topic for the content
hashes from the history of an entity. This will result in duplicated
(but idempotent) messages being sent to issue deletions, but requires
no new moving piece, or relying on the client to synchronise and
submit the deletion messages to the document topic.

The indexing nodes would also listen to this topic and perform purges
of its indexes when required. It's worth noting that apart from when
deleting a key, potentially by setting the value to nil, the key
should always be the content hash of its value.

In more advanced cases, the eviction message above could potentially
contain dates to only evict partial histories.

There's also an issue around someone else adding a new version of an
entity at the same time when the entity is evicted, which will result
in the entity to resurface. Though this is just a special case of it
happening later, which might be totally valid. An alternative is to do
a hard eviction, that stops new versions of an entity be able to
written in the future.

#### Proposal B: Evict Content Directly

Another approach is that instead of evicting a specific entity, the
user could provide a list of content hashes to explicitly evict. Any
logic or queries could be used to build this list. This wouldn't deal
with entities or their time lines explicitly.
