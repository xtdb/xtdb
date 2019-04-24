<img alt="Crux" role="img" aria-label="Crux" src="./crux-logo.svg">

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux)

Crux is an open source document database with bitemporal graph queries. Java
and Clojure APIs are provided.

Crux follows an _unbundled_ architectural approach, which means that it is
assembled from highly decoupled components through the use of semi-immutable
logs at the core of its design. Logs can currently be stored in LMDB or RocksDB
for standalone single-node deployments, or using Kafka for clustered
deployments. Indexes can currently be stored using LMDB or RocksDB.

At its core Crux is built for efficient bitemporal indexing of schemaless
documents, and this simplicity enables broad possibilities for creating layered
extensions on top, such as to add additional transaction, query, and schema
capabilities. Crux does not currently support SQL but it does provides an
EDN-based Datalog query interface that can be used to express a comprehensive
range of SQL-like join operations as well as recursive graph traversals.

## CircleCI Build

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

## Latest Version

[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux.svg)](https://clojars.org/juxt/crux)

## Documentation

Please visit our [official documentation](https://juxt.pro/crux/docs/index.html) to get started with Crux.

## Community & Contact

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux)

crux@juxt.pro

## Using Clojure

Please note that Clojure is not _required_ when using Crux.

### Building

``` sh
lein uberjar
java -jar target/crux-*-standalone.jar --help
```

### Developing

Start a REPL. To run the system with embedded Kafka and ZK:

``` clojure
(dev)
(start)
```

This will store data under `dev-storage` in the checkout directory.

### Testing

The recommended way of running the primary tests is `lein test`.

## Copyright & License
The MIT License (MIT)

Copyright © 2018-2019 JUXT LTD.

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
