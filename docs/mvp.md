## MVP Summary

### Introduction

The CRUX MVP was developed between March 19 and August 10 in 2018. At
different times 1-3 people have worked directly on the MVP code.

The decision to develop CRUX was taken by JUXT after feeling there's a
void in the market for "unbundled", to use Martin Kleppmann's phrase,
databases being able to be used in a consistent way across nodes as a
simple library to provide graph queries.

The inspiration is partly from KV stores like RocksDB and LMDB, but
CRUX is meant to provide both richer query, and also, by using Kafka
as the primary store, having nodes be able to come and go. We want to
combine the ease of use and performance of queries across a local KV
store, used as a library, with the benefits of a semi-immutable log
provided by Kafka containing all writes.

Another inspiration is Kafka Streams. While we don't use Kafka
Streams, the model of CRUX is similar - many nodes (that can come and
go) with local indexes in a KV store like RocksDB sharing master data
living in Kafka.

CRUX is ultimately a store of versioned EDN documents. The attributes
of these documents are also indexed in the local KV stores, allowing
for queries.

On the query side, CRUX uses a Datalog dialect that's a subset of
Datomic/Datascript's with some extensions. The results of CRUX queries
can be consumed lazily (partial results must at times be sorted in
memory) and CRUX is declarative in the sense that clause order doesn't
affect the query execution (there are some cases where this isn't true
if one relies on the internals).

CRUX is also a bi-temporal database, storing both business and
transaction time. This enables corrections at past business time at a
later transaction time. Queries can use both times to get consistent
reads, as long as the data hasn't been evicted.

CRUX also supports eviction of past data, to play nicely with GDRP and
similar concerns. The main transaction log topic contains only hashes,
and is never deleted. The data itself is stored in a secondary
document topic where data can be evicted by compaction. Evicted data
is also deleted from the node indexes on eviction.

Additonally, CRUX currently has a small REST API that allows one to
use CRUX in a more conventional SaaS way, deploying Kafka and query
nodes into AWS and interact with CRUX over HTTP. This mode does not
support all features.

CRUX can also be run on a single node without Kafka as a pure
library. One aim is to be able to use the same library at vastly
different sizes of deployments.

Supported KV stores are RocksDB, LMDB and an in-memory store.

### What can CRUX currently do?

CRUX can be used either as a library, usually together with Kafka, or
over HTTP. CRUX supports four write operations and a Datalog query
interface for reads. There's additionally a way to get the history of
an entity, or a document as of a specific version.

The four transaction (write) operations are as follows:

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

[:crux.tx/evict :http://dbpedia.org/resource/Pablo_Picasso
#inst "2018-05-18T09:21:52.151-00:00"]
```

The business time is optional and defaults to transaction time, which
is taken from the Kafka log. CRUX currently writes into the past at a
single point, so to overwrite several versions or a range in time,
submitting a transaction containing several operations is
needed. Eviction works a bit differently, and all versions at or
before the provided business time are evicted.

The hashes are the SHA-1 content hash of the documents. CRUX uses an
attribute `:crux.db/id` on the documents that has to line up with the
id it is submitted under. Hence, a document looks like this:

```clj
{:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
 :name "Pablo"
 :last-name "Picasso"}
```

In practice when using CRUX, one calls `crux.db/submit-tx` with a set
of transaction operations as above, where the hashes are replaced with
actual documents:

```clj
[[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
 {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
  :name "Pablo"
  :last-name "Picasso"}
 #inst "2018-05-18T09:20:27.966-00:00"]]
```

For each operation the id and the document are hashed, and this
version is submitted to the `tx-topic` in Kafka. The document itself
is submitted to the `doc-topic`, using its content hash as key. This
latter topic is compacted, which enables later deletion of documents.

If the transaction contains CAS operations, all CAS operations must
pass their pre-condition check or the entire transaction is
aborted. This happens at the query node during indexing, and not when
submitting the transaction.

CRUX stores "entities", each having a stable id, and a set of EDN
documents making up its history. Apart from EDN, there's no schema of
the documents, and no enforced concept of references. References are
simply fields where the value of an attribute is the `:crux.db/id` of
another document.

A CRUX id is a type which satisfies the `crux.index.IdToBytes`
protocol. Keywords, UUIDs, URIs and SHA-1 hex strings do this out of
the box. Note that normal strings are not considered valid ids. CRUX
will not automatically assigns ids.

All attributes will be indexed locally to enable queries. Attributes
which have vectors or sets as the values will have all their elements
indexed. CRUX does not enforce any schema. A document can change the
type of its fields at will between versions, though this isn't
recommended, as it leads to confusion at query time.

Indexing is done via the `crux.index.ValueToBytes` protocol. The
default is to take the SHA-1 of the value serialised by Nippy. Ids
index via `IdToBytes`. `Long`, `Double`, `Date` and `String` have
implementations which respect ordering while serialised to unsigned
bytes, which is what most underlying KV stores will use to order the
keys.

The above implies that values which are maps are simply indexed as
their hash. They can be used as a value in a query to find entities
like any other literal, but the contents of the map itself are opaque
to the index. "Component entities", or RDF blank nodes, must be their
own actual entities with "anonymous" ids and have explicit transaction
operations like any other entity.

CRUX also supports a few lower-level read operations, like
`crux.doc/entities-at`, `crux.doc/entity-history` for entities from
the kv and `crux.db/get-objects` to get documents from an object
store, but these internals should not be assumed to be stable APIs,
but similar functionality will be preserved.

CRUX query capability is easiest summarized via an example:

```clj
(q/q db
    '{:find  [?e2]
      :where [(follow ?e1 ?e2)]
      :args [{:?e1 :1}]
      :rules [[(follow ?e1 ?e2)
              [?e1 :follow ?e2]]
             [(follow ?e1 ?e2)
              [?e1 :follow ?t]
              (follow ?t ?e2)]]})
```

The `db` is retrieved via a call to `crux.query/db` which optionally
takes business and transaction time. The call will block until the
local index has seen the transaction time, if provided. The
`crux.query/q` takes 2 or 3 arguments, `db` and `q` but also
optionally a `snapshot` which is already opened and managed by the
caller (using `with-open` for example). This version of the call
returns a lazy sequence of the results, while the other verision
provides a set. A snapshot can be retreived from a `kv` instance via
`crux.kv-store/new-snapshot`.

The `:args` key contains a relation where each map is expected to have
the same keys. These keys are turned into logic variable symbols and
the relation is joined with the rest of the query. The elements must
implement `Comparable`.

CRUX does not support variables in the attribute position. The entity
position is hard coded to mean the `:crux.db/id` field.

The REST API provides the following paths: `/document`, `/history`,
`/query` for reads and `/tx-log` for writes. When using the REST API
the user doesn't interact directly with Kafka, but calls one of the
query nodes (potentially behind a load balancer) over HTTP to interact
with CRUX. As the query nodes might be at different points in the
index, and different queries might go to differnet nodes, there are
currently some read consistency issues that can arise here.

### How does CRUX do it?

CRUX mainly consists of two parts, the transaction and ingestion
piece, built around Kafka, and the query piece, built on top of a
local KV store such as RocksDB. The ingestion engine populates the
indexes.

#### Ingestion

On the ingestion side, the main design is to split the data into two
separate topics, the `tx-topic` and the `doc-topic`. The users don't
write directly to these topics, but use a `crux.db.TxLog` instance to
do so. Each transaction operation will be split into several messages,
where documents go into the `doc-topic` and the hashed versions of the
transactions go into the `tx-topic`.

The `tx-topic` is immutable, but the `doc-topic` is compacted, and
keyed by the documents content hashes, enabling eviction of the
data. As data can be purged for good using this mechanism, CRUX does
not lend itself to naively be used as an event sourcing mechanism, as
while the `tx-topic` will stay intact, it might refer to documents
which have since been evicted.

The ingestion side indexes both the `doc-topic` and the `tx-topic`,
into a bunch of local indexes in the KV store, which are used by the
query engine. The indexes are:

+ `content-hash->doc-index` Main document store.
+ `attribute+value+content-hash-index` Secondary index of attributes.
+ `content-hash+entity-index` Used to find entities for a content
  hash, currently this mapping is 1-1 but this was not the original
  intention.
+ `entity+bt+tt+tx-id->content-hash-index` Main temporal index, used
  to find the content hash of a specific entity version.
+ `meta-key->value-index` Used to store Kafka offsets and transaction
  times.

#### Query

The query engine is built using the concept of "virtual indexes",
which bottom out to a combination of the above physical indexes or
disk or data directly in-memory. The actual queries are represented as
a composition and combination of these indexes. Things like range
constraints are applied directly as decorators on the lower level
indexes. The query engine itself never concerns itself with time, as
this is hidden by the lower indexes.

The query is itself ultimately represented as a single n-ary join
across variables, each potentially represented by several indexes,
each combined via an unary join across them. As the resulting tree is
walked the query engine further has a concept of constraints, which
are applied to the results as the joins between the indexes are
performed. Things like predicates and sub queries are implemented
using such constraints. Nested expressions, such as `not`, `or` and
rules are executed several times as separate sub queries on the
partial results as the tree is walked. All indexes participating in a
unary join must be sorted in the same order. All n-ary indexes
(relations) participating in the parent n-ary join must have the same
variable order.

Conceptually the execution model is a combination of an n-ary worst
case optimal join and Query-Subquery (QSQ) evaluation of Datalog. The
worst case optimal join algorithm binds free variables which then are
used as arguments in QSQ. The results of the sub query are then
injected an n-ary index (relation) into the parent query, binding
further variables in the parent query ("sideways information
passing"). Rules are evaluated via a combination of eager expansion of
the rule bodies into the parent query and QSQ using caches to avoid
recusion. `or` and `or-join` are anonymous rules. `not` is a sub query
which executes when all required variables are bound, and arguments
which return results are removed from the corresponding parent result
variables.

### Known Issues

+ Rules in queries are not well tested.
+ Nested expressions in queries are not well tested.
+ Point in time semantics when writing in the past.
+ Documents requires `:crux.db/id` which removes ability to share
  versions across entities. Needs analysis.
+ Potential of inconsistent reads across different nodes when using
  REST API.
+ Queries likely to slow down with increased history size.
+ Architecture not tested in real application use.
+ Query engine is brittle to extend. Lacks internal abstractions.
+ Single attribute index complicates the query engine.
+ Lazy results requires consistent sorting across indexes, which will
  need to be able to spill over to disk.

### AWS Deployment

### Future

Phase 2, which is still to be decided, will be discussed during a few
CRUX sessions in Stockholm August 6 to 10, 2018. The minimum outcome
of the MVP if there is no phase 2 is likely that CRUX gets open
sourced in its current form.

### FAQs

**Q:** Crux or CRUX?

**A:** It is CRUX. *"CRUX feels a throwback to the 60s/70s when computers
  were UPPERCASE ONLY. Retro."* - Malcolm.


**Q:** Does CRUX support RDF/SPARQL?

**A:** No. We have a simple ingestion mechanism for RDF data in
`crux.rdf` but this is not a core feature. RDF and SPARQL support
could eventually be written as a layer on top of CRUX as a module, but
there are no plans for this by the core team.


**Q:** Does CRUX require Kafka?

**A:** Not strictly. There is a local implementation called
`crux.tx.DocTxLog` that writes transactions directly into the local KV
store. One can also implement the `crux.db.TxLog` protocol to replace
the `crux.db.KafkaTxLog` implementation. That said, Kafka is assumed
at the moment.


**Q:** What consistency does CRUX provide?

**A:** CRUX does not try to enforce consistency among nodes, which all
consume the log in the same order, but may be at different points. A
client using the same node will have a consistent view. Reading your
own writes can be achieved by providing the transaction time Kafka
assigned to the submitted transaction, which is returned in a promise
from `crux.tx/submit-tx`, in the call to `crux.query/db`. This will
block until this transaction time has been seen by the local
node.

Write consistency across nodes is provided via the `:crux.db/cas`
operation. The user needs to attempt to perform a CAS, then wait for
the transaction time (as above), and check that the entity got
updated. More advanced algorithms can be built on top of this. As
mentioned above, all CAS operations in a transaction must pass their
pre-condition check for the transaction to proceed and get indexed,
which enables one to enforce consistency across documents. There's
currently no way to check if a transaction got aborted, apart from
checking if the write succeeded.


**Q:** Does CRUX provide transaction functions?

**A:** Not directly in the MVP. But As the log is ingested in the same
order at all nodes, purely functional transformations of the tx-ops
are possible. The current transaction operations are implemented via a
multi-method, `crux.tx/tx-command` which is possible to extend with
further implementations. To make this work the spec `:crux.tx/tx-op`
also needs to be extended to accept the new operation. A transaction
command returns a map containing the keys `:kvs` `:pre-condition-fn` and
`:post-condition-fn` (the functions are optional).

**Q:** Does CRUX support the full Datomic/Datascript dialect of
Datalog?

**A:** No. The `:where` part is similar, but only the map form of
queries are supported. There's no support for Datomic's built-in
functions, or accessing the log and history directly. There's also no
support for variable bindings or multiple source vars.

Differences include that `:rules` and `:args`, which is a relation
represented as a list of maps which is joined with the query, are
being provided in the same query map as the `:find` and `:where`
clause. CRUX additionally supports the built-in `==` for unification
as well as the `!=`. Both these unification operators can also take
sets of literals as arguments, requiring at least one to match, which
is basically a form of or.

Many of these things can be expected to change after the MVP, but
compatibility is not a goal for CRUX.

**Q:** Any plans for Datalog, Cypher, Gremlin or SPARQL support?

**A:** The goal is to support different languages, and decouple the
query engine from its syntax, but this is not currently the case in
the MVP.
