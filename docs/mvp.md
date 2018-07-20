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
over HTTP. CRUX supports 4 write operations and a Datalog query
interface for reads. There's additionally a way to get the history of
an entity, or a document as of a specific version.

The four write operations are as follows:

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
submitting several operations are needed. Eviction works a bit
differently, and all versions before the provided business time are
evicted.

The hashes are the SHA-1 content hash of the documents. CRUX uses an
attribute `:crux.db/id` on the documents that is assumed to line up
with the id it is submitted under. Having this id embedded in the
document isn't strictly necessary, but some queries will require it to
exist to work. Hence, a document looks like this:

```clj
{:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
 :name "Pablo"
 :last-name "Picasso"}
```

In practice when using CRUX, one calls `crux.db/submit-tx` with a set
of write operations as above, where the hashes are replaced with
actual documents:

```clj
[[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
 {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
  :name "Pablo"
  :last-name "Picasso"}
 #inst "2018-05-18T09:20:27.966-00:00"]]
```

For each operation the id and the document are hashed, and this
version is submitted to the tx-topic in Kafka. The document itself is
submitted to the doc-topic, using its content hash as key. This latter
topic is compacted, which enables later deletion of documents.

CRUX stores "entities", each having a stable id, and a set of EDN
documents making up its history. Apart from EDN, there's no schema of
the documents, and no enforced concept of references. References are
simply fields where the value of an attribute is the id of another
document.

All attributes will be indexed locally to enable queries. Attributes
which have vectors or sets as the values will have all their elements
indexed. CRUX does not enforce any schema. A document can change the
type of its fields at will between versions, though this isn't
recommended, as it leads to confusion at query time.

CRUX also supports a few lower-level read operations, like
`crux.doc/entities-at`, `crux.doc/entity-history` for entities and
`crux.db/get-objects` to get documents, but these internals should not
be assumed to be stable APIs, but similar functionality will be
preserved.

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
local index has the transaction time, if provided. The `crux.query/q`
takes 2 or 3 arguments, `db` and `q` but also optionally a `snapshot`
which is already opened and managed by the caller (using `with-open`
for example). This version of the call returns a lazy sequence of
results, while the other verision provides a set. A snapshot can be
retreived from a `kv` instance via `crux.kv-store/new-snapshot`.

The `:args` key contains a relation where each map is expected to have
the same keys. These keys are turned into symbols and the relation is
joined with the rest of the query. The elements must implement
`Comparable`.

### How does CRUX do it?

### Implementation Details

### Known Issues

### AWS Deployment

### Future

### FAQs
