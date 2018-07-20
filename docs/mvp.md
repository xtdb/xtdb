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

### How does CRUX do it?

### Implementation Details

### Known Issues

### AWS Deployment

### Future

### FAQs
