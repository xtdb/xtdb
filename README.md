<img alt="Crux" role="img" aria-label="Crux" src="./docs/reference/modules/ROOT/images/crux-logo-banner.svg">

[Crux](https://opencrux.com) is a general purpose database with graph-oriented bitemporal indexes.
Datalog, SQL & EQL queries are supported, and Java, HTTP & Clojure APIs are
provided.

Crux follows an _unbundled_ architectural approach, which means that it is
assembled from decoupled components through the use of an immutable log and
document store at the core of its design. A range of storage options are
available for embedded usage and cloud native scaling.

Bitemporal indexing of schemaless documents enables broad possibilities for
creating layered extensions on top, such as to add additional transaction,
query, and schema capabilities. In addition to SQL, Crux supplies a
[Datalog](https://en.wikipedia.org/wiki/Datalog) query interface that can be
used to express complex joins and recursive graph traversals.

## Quick Links

* [Documentation](https://opencrux.com)
* [Maven releases - Clojars](https://clojars.org/juxt/crux-core)
  ```xml
  <dependency>
    <groupId>juxt</groupId>
    <artifactId>crux-core</artifactId>
    <version>21.05-1.17.0-beta</version>
  </dependency>
  ```

  ```clojure
  [juxt/crux-core "21.05-1.17.0-beta"]
  ```

  ```clojure
  juxt/crux-core {:mvn/version "21.05-1.17.0-beta"}
  ```
* [Release notes](https://github.com/juxt/crux/releases)
* Support: [Zulip community chat](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux) | [GitHub Discussions](https://github.com/juxt/crux/discussions) | crux@juxt.pro
* Development:
  * [Kanban board](https://github.com/juxt/crux/projects/1)
  * CircleCI build: [![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

## Unbundled Architecture

Crux embraces the transaction log as the central point of coordination when
running as a distributed system. Use of a separate document store enables simple
eviction of active and historical data to assist with technical compliance for
information privacy regulations.

> What do we have to gain from turning the database inside out? Simpler code,
> better scalability, better robustness, lower latency, and more flexibility for
> doing interesting things with data.
>
> — Martin Kleppmann

<img alt="Unbundled Architecture Diagram" role="img" aria-label="Crux Venn" src="./docs/articles/modules/ROOT/images/crux-node-1.svg" width="1000px">

This design makes it feasible and desirable to embed Crux nodes directly within
your application processes, which reduces deployment complexity and eliminates
round-trip overheads when running complex application queries.

## Repo Layout

Crux is split across multiple projects which are maintained within this
repository. `crux-core` contains the main functional components of Crux along
with interfaces for the pluggable storage components (Kafka, LMDB, RocksDB
etc.). Implementations of these storage options are located in their own
projects.

Project directories are published to Clojars independently so that you can
maintain granular dependencies on precisely the individual components needed
for your application.

## Developing Crux

See the [https://github.com/juxt/crux/tree/master/dev](dev) directory README for instructions on how to develop Crux.

## Copyright & License
The MIT License (MIT)

Copyright © 2018-2021 JUXT LTD.

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

A complete list of compiled dependencies and corresponding licenses is
maintained and available on request.
