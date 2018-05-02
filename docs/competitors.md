## Competitors

Non-exhaustive list. Apart from Datomic none of these support temporal
queries.

### Graph

#### [Neo4J](https://neo4j.com/)

_World's leading graph database, with native graph storage and
processing._

Has its own query language,
[Cypher](https://neo4j.com/developer/cypher/). It also have a [GraphQL
extension](https://neo4j.com/developer/graphql/) and a [Gremlin
module](http://tinkerpop.apache.org/docs/current/reference/#neo4j-gremlin). Supports
what they call ["causal
clustering"](https://neo4j.com/docs/operations-manual/current/clustering/causal-clustering/introduction/)
which contains of a core set of servers using Raft for transactions,
and a potentially larger set of read replicas.

Neo4J is written in Java. They are aiming to make an
[openCypher](https://www.opencypher.org/) standard.

https://db-engines.com/en/system/Neo4j

#### [DataStax](https://en.wikipedia.org/wiki/DataStax)

_DataStax Enterprise: he always-on data platform, powered by the best
distribution of Apache Cassandra._

Built on top of DSE is [DataStax Enterprise
Graph](https://docs.datastax.com/en/dse/6.0/dse-dev/datastax_enterprise/graph/graphTOC.html)
which supports [Gremlin](http://tinkerpop.apache.org/gremlin.html) and
[TinkerPop](http://tinkerpop.apache.org):

    DataStax Enterprise Graph is the first graph database fast enough to
    power customer facing applications.

DataStax Enterprise is built around Apache Cassandra which is written
in Java. TinkerPop and Cassandra are both Apache projects, and
DataStax are one of the drivers behind TinkerPop and Gremlin, see
[this
post](https://www.datastax.com/2017/12/another-graph-database-announcement-another-shout-out-for-datastax-funded-innovation).


https://db-engines.com/en/system/Datastax+Enterprise

https://db-engines.com/en/system/Cassandra

#### [Dgraph](https://github.com/dgraph-io/dgraph)

_Dgraph - a low latency, high throughput, native and distributed graph
database._

Is open source, written in Go, distributed and has a GraphQL
front-end:

    Dgraph's goal is to provide Google production level scale and
    throughput, with low enough latency to be serving real time user
    queries, over terabytes of structured data. Dgraph supports
    GraphQL-like query syntax, and responds in JSON and Protocol Buffers
    over GRPC and HTTP.

The [schema](https://docs.dgraph.io/query-language/#schema) in Dgraph
can be implicit, in which case an attribute takes the type of its
first value. Defining indexes must be done explicitly. Dgraph shards
its data based on attributes and uses Raft for consensus. It has its
own custom key value store,
[Badger](https://github.com/dgraph-io/badger).

See also [Dgraph compared to other
databases](https://docs.dgraph.io/dgraph-compared-to-other-databases/)
and [Dgraph Design Concepts](https://docs.dgraph.io/design-concepts/).

https://db-engines.com/en/system/Dgraph

#### [Cayley](https://github.com/cayleygraph/cayley)

_Cayley is an open-source graph inspired by the graph database behind
Freebase and Google's Knowledge Graph._

Cayley is written in Go. It has GraphQL and Gremlin inspired query
languages. Some RDF support.

### Datalog

#### [Datomic](https://www.datomic.com/)

_A transactional database with a flexible data model, elastic scaling,
and rich queries._

It's worth noting that the current Datomic homepage is focusing on the
auditing aspects of the time-line, and not the business time:

    Chronological: Because Datomic stores all data by default, you can
    audit how and when changes were made.

Datomic is written in Clojure.

https://db-engines.com/en/system/Datomic

#### [datahike](https://github.com/replikativ/datahike)

_A durable datalog implementation adaptable for distribution._

Datahike is a fork of
[datascript](https://github.com/tonsky/datascript), adding persistence
via
[hitchhiker-tree](https://github.com/datacrypt-project/hitchhiker-tree):

    datahike runs locally on one peer. A transactor might be provided in
    the future and can also be realized through any linearizing write
    mechanism, e.g. Apache Kafka.

Datahike is written in Clojure.

#### [Mentat](https://github.com/mozilla/mentat)

_Project Mentat is a persistent, embedded knowledge base. It draws
heavily on DataScript and Datomic._

Mentat is a Mozilla project written in Rust and supports an EDN
dialect of Datalog and data model similar to Datomic's. It's built on
top of SQLite:

    Mentat aims to offer many of the advantages of SQLite — single-file
    use, embeddability, and good performance — while building a more
    relaxed, reusable, and expressive data model on top.

#### [LogicBlox](http://www.logicblox.com/)

_LogicBlox is a smart database that combines transactions, analytics,
planning and business logic, powering a new class of smart enterprise
applications._

LogicBlox is a commercial database and rule engine written in C++,
which has its own Datalog dialect called LogiQL.

See the [Design and Implementation of the LogicBlox
System](http://www.cs.ox.ac.uk/dan.olteanu/papers/logicblox-sigmod15.pdf).

### RDF

#### [Amazon Neptune](https://aws.amazon.com/neptune/)

_Amazon Neptune is a fast, reliable, fully-managed graph database
service that makes it easy to build and run applications that work
with highly connected datasets._

Neptune is an Amazon service providing both TinkerPop for property
graph and SPARQL for RDF data, from their
[FAQs](https://aws.amazon.com/neptune/faqs/):

+ is ACID compliant with immediate consistency.
+ is designed to support graph applications that require high
  throughput and low latency graph queries. With support for up to 15
  read replicas, Amazon Neptune can support 100,000s of queries per
  second
+ does not require you to create specific indices to achieve good
  query performance, and it minimizes the need for such second
  guessing of the database design.
+ is a purpose-built, high-performance graph database engine. Neptune
  efficiently stores and navigates graph data, and uses a scale-up,
  in-memory optimized architecture to allow for fast query evaluation
  over large graphs.

Neptune is currently in limited preview, requiring sign up.

https://db-engines.com/en/system/Amazon+Neptune

#### [Stardog](https://www.stardog.com/)

_Stardog makes it fast and easy to turn enterprise data into
knowledge._

Stardog is a commercial RDF store written in Java, which supports
SPARQL and has a GraphQL front-end. Also supports TinkerPop.

https://db-engines.com/en/system/Stardog

#### [Jena](https://jena.apache.org/)

_A free and open source Java framework for building Semantic Web and
Linked Data applications._

While Jena itself is a framework, it also provides a SPARQL server
called [Fuseki](https://jena.apache.org/documentation/fuseki2/). Also
supports OWL reasoning.

### Streaming

Streaming platforms in general are not direct competitors as such, but
might be used, as we will likely be using it, to build and piece
together bespoke solutions that solve the same problems we try to do.

For a Confluent and Kafka centric view: [Putting Apache Kafka To Use:
A Practical Guide to Building a Streaming Platform (Part
1)](https://www.confluent.io/blog/stream-data-platform-1/)

How to pick between Apache's different streaming platform like Kafka,
Flink and Spark isn't obvious.

#### [Kafka Streams](https://kafka.apache.org/documentation/streams/)

_Kafka Streams is a client library for building applications and
microservices, where the input and output data are stored in Kafka
clusters._

Uses RocksDB for local KTable state. If we use Kafka for our log, we
might also use Kafka Streams as the end-to-end framework.

#### [Apache Flink](https://flink.apache.org/)

_Apache Flink® is an open-source stream processing framework for
distributed, high-performing, always-available, and accurate data
streaming applications._

It's API is based around the concepts DataSet and DataStream. Also has
a graph API called
[gelly](https://ci.apache.org/projects/flink/flink-docs-release-1.4/dev/libs/gelly/).

#### [Apache Spark](https://spark.apache.org/)

_Apache Spark™ is a unified analytics engine for large-scale data
processing._

Has many front-ends and can run on many platforms. It's basic
abstraction is a Resilient Distributed Dataset (RDD). Spark has a
graph API called [GraphX](https://spark.apache.org/graphx/).
