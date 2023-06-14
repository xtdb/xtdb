<img alt="XTDB" role="img" aria-label="XTDB" src="./docs/xtdb-logo-banner.svg">


[XTDB](https://xtdb.com) is a general purpose database with graph-oriented bitemporal indexes.
Datalog, SQL & EQL queries are supported, and Java, HTTP & Clojure APIs are
provided.

XTDB follows an _unbundled_ architectural approach, which means that it is
assembled from decoupled components through the use of an immutable log and
document store at the core of its design. A range of storage options are
available for embedded usage and cloud native scaling.

Bitemporal indexing of schemaless documents enables broad possibilities for
creating layered extensions on top, such as to add additional transaction,
query, and schema capabilities. In addition to SQL, XTDB supplies a
[Datalog](https://en.wikipedia.org/wiki/Datalog) query interface that can be
used to express complex joins and recursive graph traversals.

## Quick Links

* [Documentation](https://xtdb.com)
* [Maven releases](https://repo1.maven.org/maven2/com/xtdb/)
  ```xml
  <dependency>
    <groupId>com.xtdb</groupId>
    <artifactId>xtdb-core</artifactId>
    <version>1.23.3</version>
  </dependency>
  ```

  ```clojure
  [com.xtdb/xtdb-core "1.23.3"]
  ```

  ```clojure
  com.xtdb/xtdb-core {:mvn/version "1.23.3"}
  ```
* [Release notes](https://github.com/xtdb/xtdb/releases)
* Support: [Zulip community chat](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux) | [GitHub Discussions](https://github.com/xtdb/xtdb/discussions) | hello@xtdb.com
* [Developing XTDB](https://github.com/xtdb/xtdb/tree/main/dev)

## Unbundled Architecture

XTDB embraces the transaction log as the central point of coordination when
running as a distributed system. Use of a separate document store enables simple
eviction of active and historical data to assist with technical compliance for
information privacy regulations.

> What do we have to gain from turning the database inside out? Simpler code,
> better scalability, better robustness, lower latency, and more flexibility for
> doing interesting things with data.
>
> — Martin Kleppmann

<img alt="Unbundled Architecture Diagram" role="img" aria-label="XTDB Venn" src="./docs/concepts/modules/ROOT/images/xtdb-node-1.svg" width="1000px">

This design makes it feasible and desirable to embed XTDB nodes directly within
your application processes, which reduces deployment complexity and eliminates
round-trip overheads when running complex application queries.

## Repo Layout

XTDB is split across multiple projects which are maintained within this
repository. `core` contains the main functional components of XTDB along
with interfaces for the pluggable storage components (Kafka, LMDB, RocksDB
etc.). Implementations of these storage options are located in their own
projects.

Project directories are published to Maven independently so that you can
maintain granular dependencies on precisely the individual components needed
for your application.

## Pre-Release Snapshot Builds

Maven snapshot versions are periodically published under `dev-SNAPSHOT` and are
used to facilitate support and debugging activities during the development
cycle. To access snapshots versions, the Sonatype snapshot repository must be
added to your project definition:

```xml
<repository>
  <id>sonatype.snapshots</id>
  <name>Sonatype Snapshot Repository</name>
  <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
  <releases>
    <enabled>false</enabled>
  </releases>
  <snapshots>
    <enabled>true</enabled>
  </snapshots>
</repository>
```

```clojure
;; project.clj
:repositories [["sonatype snapshots" {:url "https://s01.oss.sonatype.org/content/repositories/snapshots"}]]
```

```clojure
;; deps.edn
:mvn/repos {"sonatype snapshots" {:url "https://s01.oss.sonatype.org/content/repositories/snapshots"}}
```

In contrast to regular releases which are immutable, a `dev-SNAPSHOT` release
can be "updated" - this mutability can often be useful but may also cause
unexpected surprises when depending on `dev-SNAPSHOT` for longer than necessary.
Snapshot versions, including full `dev-<timestamp>` coordinates (which are
useful to avoid being caught out by mutation), can be browsed
[here](https://s01.oss.sonatype.org/content/repositories/snapshots/com/xtdb/xtdb-core/dev-SNAPSHOT/).

## Copyright & License
The MIT License (MIT)

Copyright © 2018-2023 JUXT LTD.

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
