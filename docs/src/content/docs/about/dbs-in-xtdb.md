---
title: Databases in XTDB
---

<details>
  <summary>Changelog (last updated v2.2)</summary>

v2.2: single-writer indexing

: Indexing within a database is now single-writer — see ['Database architecture'](#database-architecture) above for how it works today.

  Previously, every indexer node independently consumed the log and wrote its own block files to the object store.
  The design required all indexers to produce byte-identical output, so every feature had to be expressible as a pure, deterministic function of the log — which ruled out anything that wanted to draw per-transaction metadata (e.g. tx-id, system-time) from outside the log.

  Single-writer lifts that restriction, unlocking new database topologies like change-data-capture-fed secondaries.

  No upgrade steps needed — the user-facing API is unchanged, and existing Kafka topics continue to work.

v2.1: multi-database support

: XTDB now supports multiple databases within a single XTDB cluster.

  Previously, an XTDB cluster only had a single database; XTDB clusters were entirely isolated from each other.

  v2.1 added `ATTACH DATABASE` and `DETACH DATABASE` statements.

</details>

The term 'database' is very much overloaded in the database world.

In PostgreSQL, a database is a collection of schemas, which are collections of tables.
In MySQL, a database is synonymous with a schema.
In SQLite, a database is a single file containing multiple tables.
In MongoDB, a database is a collection of collections.
The SQL specification calls these a 'catalog'.

Given this variance, let's define what this means in XTDB:

- An XTDB database is a collection of tables.
  Tables may be grouped into 'schemas' - XT has relatively little knowledge of schemas beyond tables optionally having two-part names (`schema.table`).

  If the schema is not specified, the default schema is named `public`.
- A single database in XTDB shares a [transaction log](/ops/config/log) and [object storage](/ops/config/storage) with other consumers of that database.
- Multiple XTDB nodes that share a 'primary' database (named `xtdb`) are considered an XTDB 'cluster' - they share a transaction log and object storage for that database.

  This is a logical distinction, because XTDB nodes don't know about each other - there is no communication between them except via the transaction log and object storage.
- XTDB clusters may have any number of attached 'secondary' databases, defined by the log and storage configuration.

  Each database may be safely shared by multiple XTDB clusters, simply by pointing their log and storage configuration to the same underlying resources - **databases (storage) and clusters (compute) are completely decoupled.**
- When you connect to an XTDB node, as part of the connection string, you will specify a database.
  (e.g. in JDBC: `jdbc:xtdb://localhost:5432/my-db`).

  XTDB supports cross-database queries - queries may refer to tables in other databases by using fully-qualified names (e.g. `FROM database.schema.table`).
  Unqualified tables are assumed to be from the database you connected to.
- Transactions are submitted to one database and may only refer to tables within that database.

  XTDB [guarantees serializability](/about/txs-in-xtdb) within a single database, but not between databases.
  
## What does this mean for me?

This decoupling of databases (storage) and clusters (compute) enables a **data mesh architecture** - organize your databases around business domains (orders, customers, products), while each application team runs their own compute cluster.
Teams can attach secondary databases to access shared domain data, aligning your data model with your organization's structure while keeping compute independent.

```d2
direction: up

Apps: End-User Applications {
  App1: Webapp 1
  App2: Webapp 2
  App3: Analytics Service
}

Compute: Compute - XTDB Clusters {
  Cluster1: XTDB Cluster 1 {
    Node1A: Node 1A
    Node1B: Node 1B
    Node1C: Node 1C
  }

  Cluster2: XTDB Cluster 2 {
    Node2A: Node 2A
    Node2B: Node 2B
    Node2C: Node 2C
  }
}

Storage: Storage - XTDB Databases {
  primary1: "'xtdb'\n(log + object storage)\n(cluster 1 primary)" {
    shape: cylinder
  }

  inventoryDb: "'user-preferences'\n(log + object storage)" {
    shape: cylinder
  }

  ordersDb: "'orders'\n(log + object storage)" {
    shape: cylinder
  }

  primary2: "'xtdb'\n(log + object storage)\n(cluster 2 primary)" {
    shape: cylinder
  }
}

Apps.App1 -> Compute.Cluster1
Apps.App2 -> Compute.Cluster1
Apps.App3 -> Compute.Cluster2

Compute.Cluster1 -> Storage.primary1: primary {
  style.stroke-dash: 3
}
Compute.Cluster1 -> Storage.inventoryDb: secondary {
  style.stroke-dash: 3
}
Compute.Cluster1 -> Storage.ordersDb: secondary {
  style.stroke-dash: 3
}

Compute.Cluster2 -> Storage.primary2: primary {
  style.stroke-dash: 3
}
Compute.Cluster2 -> Storage.ordersDb: secondary {
  style.stroke-dash: 3
}
```

## Database architecture

Every XTDB database is backed by three pluggable external stores — a **source log** and a **replica log** (typically Kafka topics) and an **object store** (typically S3-compatible).
A cluster of XTDB nodes is defined by — and communicates entirely through — these shared resources.

```d2
direction: right

client: Client {
  shape: person
}

source: "Source log\n(e.g. Kafka topic)" {
  shape: cylinder
}

replica: "Replica log\n(e.g. Kafka topic)" {
  shape: cylinder
}

cluster: XTDB cluster {
  leader: "Leader\n(for this database)" {
    style.bold: true
  }
  f1: Follower
  f2: Follower
}

objstore: "Object store\n(e.g. S3 / Azure / GCS)" {
  shape: cylinder
}

client -> source: "submitTx"
source -> cluster.leader: "consumes"
cluster.leader -> replica: "publishes\nresolved txns"
replica -> cluster.f1
replica -> cluster.f2
cluster.leader -> objstore: "writes blocks"
objstore -> cluster.leader
objstore -> cluster.f1
objstore -> cluster.f2
```

### External stores

XTDB relies on three pluggable external stores — the source log, replica log, and object store.
See the [log](/ops/config/log) and [storage](/ops/config/storage) configuration docs for the available implementations.

Source log

: the totally-ordered queue of client writes for this database.
  Clients append to it via `submitTx` (or SQL DML); the current leader consumes from it.

  e.g. a Kafka topic.

Replica log

: the leader's published output for this database, containing already-resolved transactions in indexed order.
  Only the current leader writes to it; followers tail it.

  e.g. a second Kafka topic, separate from the source log.

Object store

: shared storage for block files.

  e.g. AWS S3, Azure Blob Storage, GCP Cloud Storage.

### Node processing

An XTDB cluster consists of multiple XTDB nodes — each node is a single XTDB process.
For each database it serves, a node runs the following sub-components:

Leader (v2.2+)

: for each database, exactly one node in the cluster holds leadership at any given time, elected automatically across the nodes serving that database.
  If the current leader goes away, another node takes over without operator action — see ['Ingestion stopped'](/ops/troubleshooting#ingestion-stopped) for how failures are handled.

  A leader:

  - consumes the source log, resolves each transaction, and publishes the result to the replica log.
  - at the end of each block (~100k rows/4 hours), writes the block to the object store.

Follower (v2.2+)

: for each database where this node isn't the leader:

  - tails the replica log and applies the already-resolved transactions to its own in-memory index.
  - serves queries directly from that index and the blocks in the object store.
  - may be promoted to leader when the current leader goes away.

  Because followers already hold a current-state index, a follower taking over as leader is effectively a hot-standby promotion — no catch-up replay required.

Compactor

: re-sorts/re-partitions block files in the object store to make them faster to query.
  Every node participates in compaction — work is picked up at random, with compaction output being byte-identical regardless of which node produced it.

Query engine

: serves queries by reading the object store (via local caches) and recently indexed transactions from the live in-memory index.

For more details on XTDB's storage and its optimisations, check out the ['Building a Bitemporal Index'](https://xtdb.com/blog/building-a-bitemp-index-3-storage) series.

## Attaching/Detaching secondary databases (v2.1+)

Secondary databases are attached to and detached from a cluster by sending transactions to its **primary (`xtdb`) database**.

To attach a database, provide its log and storage configuration in a query to the cluster's primary database, using the [same YAML configuration format](/ops/config) as in your node configuration:

```sql
-- ensured you're connected to the `xtdb` database

-- here we're using dollar-delimited strings for the config 
-- so that we don't have to escape all the single-quotes.

ATTACH DATABASE my_secondary WITH $$
  log: !Local
    path: 'my-secondary-db/log'
  storage: !Local
    path: 'my-secondary-db/storage'

  mode: 'read-write' -- or 'read-only', v2.2+
$$
```

If you're using Kafka/S3, for example, you can create a secondary database with another topic on the same Kafka cluster, and either another S3 bucket or another directory within the same bucket:

```sql
-- assuming you've defined the 'my-kafka' cluster in your node configuration

ATTACH DATABASE my_secondary WITH $$
  log: !Kafka
    cluster: 'my-kafka'
    topic: 'xtdb.my-secondary'
    
  storage: !S3
    bucket: 'my-bucket'
    path: 'my-secondary'
$$
```

- To detach a database, send `DETACH DATABASE my_secondary`.
- **Read-only mode (v2.2+)**: specify `mode: 'read-only'` in the configuration to attach a database read-only.
  A read-only cluster still indexes transactions locally so it can serve queries, but it won't write blocks to the object store or compact — it pauses at block boundaries and picks up blocks written by another (read-write) cluster on the same database instead.

## Querying multiple databases

Once you've attached secondary databases to your cluster, you can query them either by re-connecting to that database, or by using fully-qualified names in your queries.

This query pulls in data from both the primary database and our previously created secondary database, **within the same query**:

```sql
-- connected to the primary database

SELECT *

-- refer to public.orders table in my_secondary database
FROM my_secondary.orders o 

  -- `users` from the primary database
  JOIN users u ON (o.user_id = u._id)
```

Table names may take any of the following forms:

- `table` - refers to `public.table` in the current database
- `schema.table` - refers to `schema.table` in the current database
- `database.schema.table` - refers to `schema.table` in the specified database
- `database.table` - refers to `public.table` in the specified database

`schema.table` and `database.table` may be ambiguous, of course - if they both exist, an error will be raised.
