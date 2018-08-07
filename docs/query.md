## Graph Query

### Basic Graph Queries

Crux needs to resolve and join entities as of a certain point in
bitemporal time efficiently. This is the main feature of
Crux. Secondary to this is support for normal filter and range queries
on the found entities.

Initially, Crux assumes to have all indexes needed to respond to a
query on a query node. Data might potentially be sharded in various
ways, so different query nodes see different data, or collaborate to
resolve the full query, but this is beyond the scope of the first MVP.

As the design of Crux currently assumes a transaction log feeding into
the indexes, getting the latest view of the world, and reading your
own writes requires some form of blocking to ensure the data you wrote
actually has reached the index. One approach is to simply provide an
API with timeout, waiting for a point of time to reach the query node
before executing the query.

+ [Linked Data Sets](https://www.w3.org/wiki/DataSetRDFDumps)
+ [Stanford Network Analysis
  Project](https://snap.stanford.edu/index.html)
+ [Datomic MusicBrainz sample
  database](https://github.com/Datomic/mbrainz-sample)
+ [Neo4j vs Dgraph - The numbers speak for
  themselves](https://blog.dgraph.io/post/benchmark-neo4j/)
+ [NoSQL Performance Benchmark 2018 – MongoDB, PostgreSQL, OrientDB,
  Neo4j and
  ArangoDB](https://www.arangodb.com/2018/02/nosql-performance-benchmark-2018-mongodb-postgresql-orientdb-neo4j-arangodb/)
+ [TinkerPop Provider
  Docimentation](http://tinkerpop.apache.org/docs/current/dev/provider/)
+ [Fluxgraph (tinkerpop 2 on Datomic)](https://github.com/datablend/fluxgraph)
+ [Do We Need Specialized Graph Databases? Benchmarking Real-Time
  Social Networking
  Applications](https://event.cwi.nl/grades/2017/12-Apaci.pdf)
+ [LDBC: The graph & RDF benchmark
  reference](http://www.ldbcouncil.org/)
+ [RDFox Tests](http://www.cs.ox.ac.uk/isg/tools/RDFox/2014/AAAI/)
+ [LUBM: A Benchmark for OWL Knowledge Base
  Systems](http://swat.cse.lehigh.edu/pubs/guo05a.pdf)
+ [LUBM](http://swat.cse.lehigh.edu/projects/lubm/)

### Indexes

The main complication in Crux is [bitemporality](bitemp.md). The as-of
view should be resolved for both business and transaction time, both
defaulting to now, without the user having to deal with these temporal
aspects in the query or schema. This is one of Crux main
differentiating features.

One option is to store indexes similar to how it's done in this
article: [SQL in CockroachDB: Mapping Table Data to Key-Value
Storage](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)

Time could be represented as two additional longs, one for business
and one for transaction time, at the end of the key. It's likely
faster if this time is "reversed", so later dates come first in the
index. Another alternative is an [multi dimensional
index](https://redis.io/topics/indexes#multi-dimensional-indexes). There
are many variants on this, like
[R-trees](https://en.wikipedia.org/wiki/R-tree), but most of them are
costly to build.

We want the indexes both to be fast, and also decoupled from a single
immutable time-line. Especially, corrections of the past should not
cost more than any other indexing. Deletions at any point should also
be simple. Indexes should be possible to backup and resume from the
log, so the entire index doesn't need to be rebuilt from scratch in
case of query node failure.

The strategy of building indexes and what's stored in them should be
decoupled from the way the indexes actually are built and stored. That
is, it should be simple to swap out the underlying store or
stores. This doesn't mean this necessarily should be simple to do
while running, which would be a post MVP feature.

+ [Secondary indexing with Redis](https://redis.io/topics/indexes)
+ [Leapfrog Triejoin: a worst-case optimal join
  algorithm](https://arxiv.org/abs/1210.0481)
+ [LogicBlox Technical Report: Leapfrog Triejoin: A Worst-Case Optimal
  Join
  Algorithm](https://developer.logicblox.com/wp-content/uploads/2013/10/LB1201_LeapfrogTriejoin.pdf)
+ [On the Way to Better SQL Joins in
  CockroachDB](https://www.cockroachlabs.com/blog/better-sql-joins-in-cockroachdb/)
+ [Hippo: A Fast, yet Scalable, Database Indexing
  Approach](https://arxiv.org/abs/1604.03234)
+ [A-Tree: A Bounded Approximate Index
  Structurqe](https://arxiv.org/abs/1801.10207)
+ [Spatial index](https://en.wikipedia.org/wiki/Spatial_index)
+ [Dgraph Design Concepts](https://docs.dgraph.io/design-concepts/)
+ [ZipG: A Memory-efficient Graph Store for Interactive
  Queries](https://people.eecs.berkeley.edu/~anuragk/papers/zipg.pdf)
+ [Succinct: Enabling Queries on Compressed
  Data](https://people.eecs.berkeley.edu/~anuragk/succinct-techreport.pdf)
+ [TAO: Facebook’s Distributed Data Store for the Social
  Graph](https://www.usenix.org/system/files/conference/atc13/atc13-bronson.pdf)
+ [An overview of Neo4j
  Internals](https://www.slideshare.net/thobe/an-overview-of-neo4j-internals)
+ [Index Free Adjacency or Hybrid Indexes for Graph
  Databases](https://www.arangodb.com/2016/04/index-free-adjacency-hybrid-indexes-graph-databases/)
+ [Inside DSE Graph: What Powers the Best Enterprise Graph
  DB](https://www.datastax.com/2016/08/inside-dse-graph-what-powers-the-best-enterprise-graph-database)
+ [Adaptive Tuple Differential
  Coding](https://www.researchgate.net/publication/221465140_Adaptive_Tuple_Differential_Coding)
+ [Orderly: Schema and type system for creating sortable
  byte](https://github.com/ndimiduk/orderly)
+ [Integrating Datalog and Constraint
  Solving](https://arxiv.org/abs/1307.4635)
+ [Using Datalog with Binary Decision Diagrams for Program
Analysis](https://people.csail.mit.edu/mcarbin/papers/aplas05.pdf)
+ [On Fast Large-Scale Program Analysis in
  Datalog](http://discovery.ucl.ac.uk/1474713/1/main.pdf)
+ [Efficient Lazy Evaluation of Rule-Based
Programs](https://pdfs.semanticscholar.org/004c/2bd66cc6e8aeb9f03c0ea88041d05981acb6.pdf)
+ [The LEAPS
  Algorithms](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.96.5371&rep=rep1&type=pdf)
+ [Magic sets and other strange ways to implement logic
  programs](https://web.archive.org/web/20120308104055/http://ssdi.di.fct.unl.pt/krr/docs/magicsets.pdf)
+ [What You Always Wanted to Know About Datalog (And Never Dared to
Ask)](https://pdfs.semanticscholar.org/9374/f0da312f3ba77fa840071d68935a28cba364.pdf)
+ [Magic functions: A technique to optimize extended datalog recursive
  programs](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.104.1950&rep=rep1&type=pdf)
+ [Survey and Taxonomy of Lossless Graph Compression and
  Space-Efficient Graph
  Representations](https://arxiv.org/abs/1806.01799)
+ [Compact Data Structures and Query Processing for Temporal
  Graphs](https://github.com/diegocaro/temporalgraphs/blob/master/docs/index.md)
+ [TripleBit: a Fast and Compact System for Large Scale RDF
  Data](http://www.vldb.org/pvldb/vol6/p517-yuan.pdf)
+ [Foundations of
  Databases](http://webdam.inria.fr/Alice/)
+ [A relatively simple Datalog engine in
  Rust](https://github.com/frankmcsherry/blog/blob/master/posts/2018-05-19.md)
+ [Differential
  datalog](https://github.com/frankmcsherry/blog/blob/master/posts/2016-06-21.md)
+ [A shallow dive into DataScript
  internals](http://tonsky.me/blog/datascript-internals/)
+ [Joins via Geometric Resolutions: Worst-case and
  Beyond](https://arxiv.org/abs/1404.0703)
+ [Compressed Representation of Dynamic Binary Relations with
  Applications](https://arxiv.org/abs/1707.02769)
+ [A succinct data structure for self-indexing ternary
  relations](https://arxiv.org/abs/1707.02759)
+ [Compact representation of Web graphs with extended
  functionality](http://repositorio.uchile.cl/bitstream/handle/2250/126520/Compact%20representation%20of%20Webgraphs%20with%20extended%20functionality.pdf?sequence=1q)
+ [SuRF: Practical Range Query Filtering with Fast Succinct
  Tries](https://www.cs.cmu.edu/~pavlo/papers/mod601-zhangA-hm.pdf)

### Retention

The indexes need to respect the [retention](retention.md) rules setup
for the data itself. As the indexes will contain the decrypted values
of all indexed values, being easily able to derive where a value came
from and if this now needs to be dropped must be possible.

It should further be possible to compact the indexes and support
rolling time windows. Different points in time might have different
fidelity in the index, for example keeping all of the recent data
while rolling up data on a hourly or daily basis further back in
time. This can be done both to save space and for performance reasons
by keeping the indexes smaller and mainly contain data that is likely
to be queried. See also [schema](schema.md).

Different query nodes could have different retention strategies.

### Query Language

**Crux ability to query the graph is separate from the syntax of doing
so.**

The important thing is finding and defining the interface between the
indexes and the query front end. Crux might support several query
languages as well as API level index and query engine access. As Crux
will be open source, it should be easy to reuse and understand how the
initial reference implementation provided by JUXT actually works, and
extending or deviate from it at each level.

We could use an EDN-based dialect of Datalog. There is the
consideration of made the queries clause order sensitive. This has
some benefits, as it's easier to reason about and doesn't require an
advanced query planner, but also drawbacks, as it makes the query
language less declarative, requiring understanding of index-internals
to tweak queries.

[Datalog](https://en.wikipedia.org/wiki/Datalog) is a subset of
Prolog, and we could stay closer to that. Other alternatives
are
[Cypher](https://en.wikipedia.org/wiki/Cypher_Query_Language),
[Gremlin](https://en.wikipedia.org/wiki/Gremlin_(programming_language))
and [SPARQL](https://en.wikipedia.org/wiki/SPARQL).

**Note: unlike these languages, GraphQL isn't an actual query
language. GraphQL requires extensions to do ad-hoc queries.**

+ [Dedalus: Datalog in Time and
  Space](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html)
+ [SPARQL
  S-Expressions](https://jena.apache.org/documentation/notes/sse.html)
+ [Gremlinator: An effort towards converting SPARQL queries to Gremlin
  Graph Pattern Matching
  Traversals](https://github.com/LITMUS-Benchmark-Suite/sparql-to-gremlin)
+ [neo4j-graphql-js (GraphQL to Cypher query execution
  layer)](https://github.com/neo4j-graphql/neo4j-graphql-js)
+ [Ogre is a Clojure Gremlin Language
  Variant](https://github.com/clojurewerkz/ogre)
+ [The Ubiquity of Large Graphs and Surprising Challenges of Graph
Processing](http://www.vldb.org/pvldb/vol11/p420-sahu.pdf)
+ [RETRO: A Framework for Semantics Preserving SQL-to-SPARQL
  Translation](https://www.utdallas.edu/~bxt043000/Publications/Technical-Reports/UTDCS-22-11.pdf)
+ [sql-gremlin: Provides a SQL interface to your TinkerPop enabled
  graph db](https://github.com/twilmes/sql-gremlin)
+ [Stardog 5: GraphQL Queries](https://www.stardog.com/docs/#_graphql_queries)
+ [hypergraphql: GraphQL interface for querying and serving linked
  data on the
  Web](https://github.com/semantic-integration/hypergraphql)
