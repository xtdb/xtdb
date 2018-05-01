## Competitors

Non-exhaustive list. Apart from Datomic none of these support temporal
queries.

### Graph

#### [Neo4J](https://neo4j.com/)

_World's leading graph database, with native graph storage and
processing._

Has its own query language,
[Cypher](https://neo4j.com/developer/cypher/), and also a [GraphQL
extension](https://neo4j.com/developer/graphql/). Supports what they
call ["causal
clustering"](https://neo4j.com/docs/operations-manual/current/clustering/causal-clustering/introduction/)
which contains of a core set of servers using Raft for transactions,
and a potentially larger set of read replicas.

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

https://db-engines.com/en/system/Datastax+Enterprise
https://db-engines.com/en/system/Cassandra

#### [Dgraph](https://github.com/dgraph-io/dgraph)

_Dgraph - a low latency, high throughput, native and distributed graph
database._

Is open source, distributed and has a GraphQL front-end:

    Dgraph's goal is to provide Google production level scale and
    throughput, with low enough latency to be serving real time user
    queries, over terabytes of structured data. Dgraph supports
    GraphQL-like query syntax, and responds in JSON and Protocol Buffers
    over GRPC and HTTP.

https://db-engines.com/en/system/Dgraph

### Datalog

#### [Datomic](https://www.datomic.com/)

_A transactional database with a flexible data model, elastic scaling,
and rich queries._

It's worth noting that the current Datomic homepage focusing on
auditing aspects of the time-line, and not the business time:

    Chronological: Because Datomic stores all data by default, you can
    audit how and when changes were made.

https://db-engines.com/en/system/Datomic

#### [datahike](https://github.com/replikativ/datahike)

_A durable datalog implementation adaptable for distribution._

#### [Mentat](https://github.com/mozilla/mentat)

_Project Mentat is a persistent, embedded knowledge base. It draws
heavily on DataScript and Datomic._

#### [LogicBlox](http://www.logicblox.com/)

_LogicBlox is a smart database that combines transactions, analytics,
planning and business logic, powering a new class of smart enterprise
applications._

LogicBlox is a commercial database and rule engine, which has its own
Datalog dialect called LogiQL, which see the [Design and
Implementation of the LogicBlox
System](http://www.cs.ox.ac.uk/dan.olteanu/papers/logicblox-sigmod15.pdf).

### RDF

#### [Amazon Neptune](https://aws.amazon.com/neptune/)

_Amazon Neptune is a fast, reliable, fully-managed graph database
service that makes it easy to build and run applications that work
with highly connected datasets._

https://db-engines.com/en/system/Amazon+Neptune

#### [Stardog](https://www.stardog.com/)

_Stardog makes it fast and easy to turn enterprise data into
knowledge._

Stardog is a commercial RDF store, which supports SPARQL and has a
GraphQL front-end.

https://db-engines.com/en/system/Stardog

#### [Jena](https://jena.apache.org/)

_A free and open source Java framework for building Semantic Web and
Linked Data applications._

While Jena itself is a framework, it also provides a SPARQL server
called [Fuseki](https://jena.apache.org/documentation/fuseki2/).

### Streaming

#### [Kafka Streams](https://kafka.apache.org/documentation/streams/)

_Kafka Streams is a client library for building applications and
microservices, where the input and output data are stored in Kafka
clusters._

Streaming platforms in general are not direct competitors as such, but
might be used, as we will likely be using it, to build and piece
together bespoke solutions that solve the same problems we try to do.
