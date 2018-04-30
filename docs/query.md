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

### Indexes

The main complication in Crux is [bitemporality](bitemp.md). This
should be provided as a context to the query by the user, and the as
of view should be automatically resolved for both business and
transaction time for the user. This is one of Crux main
differentiating features.

One option is to store indexes similar to how it's done in this
article: [SQL in CockroachDB: Mapping Table Data to Key-Value
Storage](https://www.cockroachlabs.com/blog/sql-in-cockroachdb-mapping-table-data-to-key-value-storage/)

Time could be represented as two additional longs, one for business
and one for transaction time, at the end of the key. It's likely
faster if this time is "reversed", so later dates come first in the
index. Another alternative is an [multi dimensional
index](https://redis.io/topics/indexes#multi-dimensional-indexes). There
are many variants on this, but most of them are costly to build.

We want the indexes both to be faster to build than Datomic, and also
decoupled from the bitemporal time lines. Especially, corrections of
the past should not cost more than any other indexing. Deletions at
any point should also be simple. Indexes should be possible to backup
and resume from the log, so the entire index doesn't need to be
rebuilt from scratch in case of query node failure.

The strategy of building indexes and what's stored in them should be
decoupled from the way the indexes actually are built and stored. That
is, it should be simple to swap out the underlying store or
stores. This doesn't mean this necessarily should be simple to do
while running, which would be a post MVP feature.

+ [Secondary indexing with Redis](https://redis.io/topics/indexes)
+ [Leapfrog Triejoin: a worst-case optimal join
  algorithm](https://arxiv.org/abs/1210.0481)
+ [On the Way to Better SQL Joins in CockroachDB](https://www.cockroachlabs.com/blog/better-sql-joins-in-cockroachdb/)
+ [Hippo: A Fast, yet Scalable, Database Indexing
  Approach](https://arxiv.org/abs/1604.03234)
+ [A-Tree: A Bounded Approximate Index
  Structure](https://arxiv.org/abs/1801.10207)

### Retention

The indexes need to respect the [retention](retention.md) rules setup
for the data itself. As the indexes will contain the decrypted values
of all indexed values, being easily able to derive where a value came
from and if this now needs to be dropped must be possible.

### Query Language

**Crux ability to query the graph is separate from the syntax of doing
so.**

The important thing is finding and defining the interface between the
indexes and the query front end. Crux might support several query
languages as well as API level index and query engine access. As Crux
will be open source, it should be easy to reuse and understand how the
reference implementation provided by JUXT actually works, and
extending or deviate from it at each level.

Datomic uses an EDN-based dialect of Datalog, with various syntactical
extensions. It's also clause order sensitive. This has some benefits,
as it's easier to reason about and doesn't require an advanced query
planner, but also drawbacks, as it makes the query language less
declarative, requiring understanding of Datomic index-internals to
tweak queries.

In reality, [Datalog](https://en.wikipedia.org/wiki/Datalog) is a
subset of Prolog, and we could stay closer to that. Other alternatives
are [Cypher](https://en.wikipedia.org/wiki/Cypher_Query_Language),
[Gremlin](https://en.wikipedia.org/wiki/Gremlin_(programming_language))
and [SPARQL](https://en.wikipedia.org/wiki/SPARQL).

**Note: unlike these languages, GraphQL isn't an actual query
language. GraphQL requires extensions to do ad-hoc queries.**

+ [Dedalus: Datalog in Time and
  Space](https://www2.eecs.berkeley.edu/Pubs/TechRpts/2009/EECS-2009-173.html)
+ [SPARQL
  S-Expressions](https://jena.apache.org/documentation/notes/sse.html)

### Datascript Compatibility

#### Query Arguments

Datascript queries supply parameters as arguments outside the main query.

For example

````
(query
{:find ['e]
 :in [$name]
 :where [['e :name $name]]}
 "Ivan")
````

As oppose to

````
{:find ['e]
 :where [['e :name "Ivan"]]}

````

If you use parameterised queries, you can of course optimise the queries (saves on parsing and validation).

I'm yet to discover if there is another reason. Crux may want to hold off on a view here, until the technical case is made, or another reason found.
