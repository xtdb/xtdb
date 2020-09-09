<img alt="Crux" role="img" aria-label="Crux" src="./docs/reference/modules/ROOT/images/crux-logo-banner.svg">

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux)

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

## CircleCI Build

[![CircleCI](https://circleci.com/gh/juxt/crux.svg?style=svg&circle-token=867b84b6d1b4dfff332773f771457349529aee8b)](https://circleci.com/gh/juxt/crux)

## Latest Release

Maven coordinates for the [Clojars Repository](https://clojars.org/juxt/crux-core):

```xml
<dependency>
  <groupId>juxt</groupId>
  <artifactId>crux-core</artifactId>
  <version>20.09-1.11.0-beta</version>
</dependency>
```
[![Clojars Project](https://img.shields.io/clojars/v/juxt/crux-core.svg?style=for-the-badge)](https://clojars.org/juxt/crux-core)

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

<img alt="Unbundled Architecture Diagram" role="img" aria-label="Crux Venn" src="./docs/about/modules/ROOT/images/crux-node-1.svg" width="1000px">

This design makes it feasible and desirable to embed Crux nodes directly within
your application processes, which reduces deployment complexity and eliminates
round-trip overheads when running complex application queries.

## Documentation

Please visit [the docs](https://opencrux.com) to get started with Crux.

## Community & Contact

We use Zulip for our main community chat:

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux)

Feel free to say hello: crux@juxt.pro

## Repo Layout

Crux is split across multiple projects which are maintained within this
repository. `crux-core` contains the main functional components of Crux along
with interfaces for the pluggable storage components (Kafka, LMDB, RocksDB
etc.). Implementations of these storage options are located in their own
projects.

Project directories are published to Clojars independently so that you can
maintain granular dependencies on precisely the individual components needed
for your application.

### Developing inside this Crux project

 The top-level project ties all the other projects together for convenience
 whilst working within this repo.

* To run a Clojure REPL that includes dependencies for all components of Crux, first build the sub-modules using `lein sub install`.
* Start a REPL with `lein repl` (with `--headless` if you're just going to connect to it from your editor).
* Once you've connected to the REPL, in the `user` namespace, run:
  * `(go)` to start up the dev node
  * `(halt!)` to stop it
  * `(reset)` to stop it, reload changed namespaces, and restart it
  * `(reset-all)` to stop it, reload all namespaces, and restart it
  * if you're using Emacs/CIDER, `cider-ns-refresh` will do all this for you - `C-c M-n M-r`, `, s x` in Spacemacs
  * Conjure users can use `ConjureRefresh`, see the [docs](https://github.com/Olical/conjure#mappings) for bindings
  * see [Integrant REPL](https://github.com/weavejester/integrant-repl) for more details.
* You should now have a running Crux node under `(user/crux-node)` - you can verify this by calling `(crux/status (crux-node))` (in the `user` namespace).
* Most of the time, you shouldn't need to bounce the REPL, but:
  * if you add a module, or change any of the dependencies of any of the modules, that'll require another `lein sub install` and a REPL bounce
  * if you change any of the Java classes, that'll require a `lein sub javac` and a REPL bounce
  * otherwise, `(user/reset)` (or just `(reset)` if you're already in the `user` ns) should be sufficient.
* You can run module tests from the root of the git repo without a `lein sub install`, because of the lein checkouts - all of the tests are in scope here, so things like `lein test :only crux.tx-test` should also work.
* Please don't put any more side-effecting top-level code in dev namespaces - you'll break this reload ability and make me sad.

### Testing

The recommended way of running the full test suite is `lein build`.

The test suite relies on the `timeout` command line utility, this comes as a default on Linux but isn't preinstalled on MacOS. You can get it with `brew install coreutils && echo 'alias timeout=gtimeout' >> ~/.bashrc'`

## Copyright & License
The MIT License (MIT)

Copyright © 2018-2020 JUXT LTD.

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
