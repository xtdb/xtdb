<img alt="Crux" role="img" aria-label="Crux" src="./docs/img/crux-logo-banner.svg">

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux)

Crux is an open source document database with bitemporal graph queries. Java,
Clojure and HTTP APIs are provided.

Crux follows an _unbundled_ architectural approach, which means that it is
assembled from highly decoupled components through the use of semi-immutable
logs at the core of its design. Logs can currently be stored in LMDB or RocksDB
for standalone single-node deployments, or using Kafka for clustered
deployments. Indexes can currently be stored using LMDB or RocksDB.

Crux is built for efficient bitemporal indexing of schemaless documents, and
this simplicity enables broad possibilities for creating layered extensions on
top, such as to add additional transaction, query, and schema capabilities.
Crux does not currently support SQL but it does provide an EDN-based
[Datalog](https://en.wikipedia.org/wiki/Datalog) query interface that can be
used to express a comprehensive range of SQL-like join operations as well as
recursive graph traversals.

Crux has been available as a *Public Alpha* since 19<sup>th</sup> April 2019.
The Public Alpha period will continue until Crux is released as a Generally
Available open source software product by JUXT later in 2019.


## CircleCI Build

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

## Latest Release

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-core.svg)](https://clojars.org/juxt/crux-core)

## Unbundled Architecture

Crux embraces the transaction log as the central point of coordination when
running as a distributed system. Use of a separate document log enables simple
eviction of active and historical data to assist with technical compliance for
information privacy regulations.

> What do we have to gain from turning the database inside out? Simpler code,
> better scalability, better robustness, lower latency, and more flexibility for
> doing interesting things with data.
>
> — Martin Kleppmann (2014)

<img alt="Unbundled Architecture Diagram" role="img" aria-label="Crux Venn" src="./docs/img/crux-node-1.svg" width="1000px">

This design makes it feasible and desirable to embed Crux nodes directly in
your application processes, which reduces deployment complexity and eliminates
round-trip overheads when running complex application queries.

###

## Data Model

<img alt="Document database with graph queries" role="img" aria-label="Crux Venn" src="./docs/img/crux-venn-1.svg" width="500px">

Crux is fundamentally a store of versioned EDN documents. The only requirement
is that you specify a valid `:crux.db/id` key which links the documents to
their corresponding entities. The fields within these documents are
automatically indexed as Entity-Attribute-Value triples to support efficient
graph queries. Document versions are indexed by `valid-time` (in addition to
`transaction-time`) which allows you to model updates into the past, present or
future.

Crux supports a Datalog query interface for reading data and traversing
relationships across all documents. Queries are executed so that the results
are lazily streamed from the underlying indexes. Queries can be made against
consistent point-in-time snapshots of your database from any Crux node
connected to the same transaction log, by specifying `transaction-time` and/or
`valid-time`.

## Documentation

Please visit our [official
documentation](https://juxt.pro/crux/docs/index.html) to get started with Crux.

## Try it with Docker
See [standalone webservice
example](https://github.com/juxt/crux/tree/master/docs/example/standalone_webservice)
for a demo Docker container.

## Community & Contact

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux)

### 3rd Party Modules
* [avisi-apps/crux-xodus](https://github.com/avisi-apps/crux-xodus) - pure-JVM
  alternative to `crux-rocksdb` and `crux-lmdb`
* [avisi-apps/crux-active-objects](https://github.com/avisi-apps/crux-active-objects)
  - TxLog implementation backed by Active Objects for use inside Atlassian
    Addons

### Companies using Crux

* [Oscaro (eCommerce)](https://www.oscaro.com)
* [Avisi (AtlasCRM)](https://www.avisi.nl/blog/crux-our-final-database-migration) - ["Crux: Our Final Database Migration"](https://www.avisi.nl/blog/crux-our-final-database-migration)
* [Gnurdle (consulting)](https://gnurdle.github.io/HoppyResume2017.html)
* Yours? Let us know :)

crux@juxt.pro

## Repo Layout

Crux is split across multiple projects which are maintained within this
repository. `crux-core` contains the main functional components of Crux along
with interfaces for the pluggable storage components (Kafka, LMDB, RocksDB
etc.). Implementations of these storage options are located in their own
projects.

Project directories are published to Clojars independently so that you can
maintain granular dependencies on precisely the individual components needed
for your application. Alternatively you can depend on `crux-uberjar` whilst in
development to spend less time worrying about which parts of Crux you need now
or in the future.

### Core

#### crux-core

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-core.svg)](https://clojars.org/juxt/crux-core)

### Cluster Node storage

#### crux-kafka

For scalability and durability.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-kafka.svg)](https://clojars.org/juxt/crux-kafka)

#### crux-kafka-embedded

Useful for experimentation and testing.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-kafka-embedded.svg)](https://clojars.org/juxt/crux-kafka-embedded)

### Local KV and Standalone mode storage

#### crux-lmdb

Better read performance for intensive querying.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-lmdb.svg)](https://clojars.org/juxt/crux-lmdb)

#### crux-rocksdb

Better write performance for heavy ingestion.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-rocksdb.svg)](https://clojars.org/juxt/crux-rocksdb)

`crux-rocksdb` is a good default choice.

### HTTP

#### Server

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-http-server.svg)](https://clojars.org/juxt/crux-http-server)

#### Client

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-http-client.svg)](https://clojars.org/juxt/crux-http-client)

### Additional

#### crux-decorators

Aggregation decorator and experimental API composition.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-decorators.svg)](https://clojars.org/juxt/crux-decorators)

#### crux-rdf

Import RDF data and run a subset of SPARQL queries.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-rdf.svg)](https://clojars.org/juxt/crux-rdf)

### Uberjar

#### crux-uberjar

One dependency to rule them all.

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-uberjar.svg)](https://clojars.org/juxt/crux-uberjar)

## Using Clojure

Please note that Clojure is not _required_ when using Crux. HTTP and Java
APIs are also available.

### REPL

Launch a REPL using the very latest Clojars `-SNAPSHOT` release:

``` sh
clj -Sdeps '{:deps {juxt/crux-core {:mvn/version "RELEASE"}}}'
```

Start a standalone in-memory (i.e. not persisted anywhere) node:

``` clojure
(require '[crux.api :as crux])
(import '[crux.api ICruxAPI])

(def my-node
  (crux/start-node
    {:crux.node/topology :crux.standalone/topology
     :crux.node/kv-store "crux.kv.memdb.MemKv" ; see docs for LMDB/RocksDB storage options
     :crux.standalone/event-log-dir "data/event-log-dir-1"
     :crux.kv/db-dir "data/db-dir-1"}))
```

`put` a document:

``` clojure
(def my-document
  {:crux.db/id :some/fancy-id
   :arbitrary-key ["an untyped value" 123]
   :nested-map {"and values" :can-be-arbitrarily-nested}})

(crux/submit-tx my-node [[:crux.tx/put my-document]])
```

Take an immutable snapshot of the database:

``` clojure
(def my-db (crux/db my-node))
```

Retrieve the current version of the document:

``` clojure
(crux/entity my-db :some/fancy-id)
```

### Development "uber" REPL

To run a REPL that includes depedencies for all components of Crux, `cd` into
`crux-dev` and run `lein repl`

### Testing

The recommended way of running the primary tests is `lein build`.

## Copyright & License
The MIT License (MIT)

Copyright © 2018-2019 JUXT LTD.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

### Dependencies

A list of compiled dependencies and corresponding licenses is available
[here](LICENSE-deps.adoc).
