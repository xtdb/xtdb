---
title: Kafka
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: single-writer support — two topics per database

: [Single-writer indexing](/about/dbs-in-xtdb#database-architecture) requires two Kafka topics per database: a **source log** for client writes and a **replica log** for the indexing leader's resolved output.
  Each database elects its own leader in its replica topic, and fences split-brain writes to that topic by term — see ['Leader election and fencing'](#leader-election-and-fencing) below.

  Previously, a database used a single Kafka topic, and every indexer node consumed it independently.
  With single-writer, only the elected leader consumes the source topic; followers tail the replica topic instead.

  Upgrading:

  - The replica topic defaults to `${topic}-replica` and auto-creates when `autoCreateTopic` is enabled, so existing deployments need no configuration changes to pick it up.
  - ACL-restricted topics need `Describe` / `Read` / `Write` on both the source and replica topics.

v2.2: `logClusters` renamed to `remotes`

: The Kafka cluster is now declared under [`remotes`](/ops/config#remotes) rather than `logClusters`.

  `logClusters` is deprecated but still honoured, so existing config keeps working — rename to `remotes` when convenient.

v2.1: multi-database support

: As part of multi-database support, `logClusters` were extracted in v2.1.

  Prior to that, the configuration in `logClusters` was within the `log`:

  ``` yaml
  log: !Kafka
    bootstrapServers: "localhost:9092"
    topic: "xtdb-log"
    # autoCreateTopic: true
    # propertiesFile: "kafka.properties"
    # propertiesMap:

  # became

  logClusters:
    kafkaCluster: !Kafka
  bootstrapServers: "localhost:9092"
      # propertiesFile: "kafka.properties"
      # propertiesMap:

  log: !Kafka
    cluster: kafkaCluster
    topic: "xtdb-log"
    # autoCreateTopic: true
  ```
  
</details>

[Apache Kafka](https://kafka.apache.org/) can be used as XTDB's message log.
Each database uses two Kafka topics — a **source log** for client writes and a **replica log** for the indexing leader's resolved output — plus Kafka's consumer-group protocol to elect the leader for that database automatically.
See ['Database architecture'](/about/dbs-in-xtdb#database-architecture) for the concepts; this page covers how to set Kafka up to back them.

## Setup

1. Add a dependency to the `com.xtdb/xtdb-kafka` module in your dependency manager.
2. On your Kafka cluster, XTDB requires **two topics per database** — a source log and a replica log:
    - Both can be created manually and provided to the node config, or XTDB can create them automatically.
    - If allowing XTDB to create the topics **automatically**, ensure that the connection properties supplied to the XTDB node have the appropriate permissions to create topics — XTDB will create each with the expected configuration values (single partition, `LogAppendTime` timestamps).
      Auto-created topics are **unreplicated**, so create them yourself for production.
3. Configure the topics and the broker — see [Settings](#settings) for which of these XTDB sets for you and which are yours.

4. XTDB should be configured to use the topics, and the Kafka cluster they're hosted on.
  It should also be authorised to perform all of the necessary operations on both.
    - For configuring the Kafka module to authenticate with the Kafka cluster, use the `propertiesFile` or `propertiesMap` configuration options to supply the necessary connection properties.
      See the [example configuration](#auth_example) below.
    - If the Kafka cluster is using **ACLs**, the XTDB node needs:
        - `Describe` / `Read` / `Write` on **both** the source and replica topics.

## Settings

Both topics — source and replica — take the same settings.

XTDB applies the topic settings it depends on only when it creates a topic itself (`autoCreateTopic: true`), so a topic you pre-create is entirely yours to configure.
The one setting it verifies on a topic that already exists is the partition count; the node refuses to start otherwise.

| Setting | Scope | Set by | Value |
| --- | --- | --- | --- |
| partition count | topic | XTDB on create, **verified** on an existing topic | Exactly `1`. A single partition is what makes the log strictly ordered and lets leader election assign it to one consumer at a time. |
| `message.timestamp.type` | topic | XTDB on create, **not** verified afterwards | `LogAppendTime`, so a record's timestamp is when the broker appended it rather than when a producer sent it. Set it yourself on a pre-created topic. |
| replication factor | topic | XTDB creates with `1` | Pre-create the topic with `3` or more for production — auto-create is unreplicated. |
| `min.insync.replicas` | topic | You | `> 1`, to make writes quorum-acknowledged. XTDB warns on startup if a topic with more than one replica leaves this at `1`, since `acks=all` then means only the partition leader. |
| `unclean.leader.election.enable` | topic or broker | You | `false`, which is Kafka's own default. `true` lets an out-of-sync replica become leader and truncate records XTDB has already been told are durable. XTDB warns on startup if a topic with more than one replica permits it. |
| `retention.ms` | topic | You | Messages need not live on the log permanently. The default of 1 day suits most deployments; 1 week is a reasonable starting point where extra caution against data loss is wanted. |
| `max.message.bytes` | topic | You | The 1MB default is fit for purpose unless your transactions are larger. |
| `cleanup.policy` | topic | You | Leave at the default `delete` — XTDB never reads compacted messages. |

XTDB also sets its own producer and consumer properties — idempotent, `acks=all` writes, `read_committed` reads, `auto.offset.reset=none`, cooperative sticky assignment, and offset commits that keep the leader-election group alive.
`propertiesMap` and `propertiesFile` can override these, but they are chosen deliberately and overriding them can break leader election.

`acks` is the exception: XTDB applies `acks=all` last, so an entry in `propertiesMap` or `propertiesFile` is logged as disregarded rather than honoured.
A write acknowledged before any follower holds it can be truncated away afterwards, which would let two nodes reach different conclusions about which of them leads a database — see [Leader election and fencing](#leader-election-and-fencing).

## Configuration

To use the Kafka module, include the following in your node configuration:

``` yaml
## We first declare the Kafka cluster under `remotes`:

remotes:
  # You can define multiple Kafka clusters here, and refer to them by name in the log configuration.
  # Here we define a single Kafka cluster named "kafkaCluster".
  kafkaCluster: !Kafka
    # -- required

    # A comma-separated list of host:port pairs to use for establishing the
    # initial connection to the Kafka cluster.
    # (Can be set as an !Env value)
    bootstrapServers: "localhost:9092"

    # Path to a Java properties file containing Kafka connection properties,
    # supplied directly to the Kafka client.
    # (Can be set as an !Env value)
    # propertiesFile: "kafka.properties"

    # A map of Kafka connection properties, supplied directly to the Kafka client.
    # propertiesMap:


## For the database, we then create a log using the Kafka cluster we just defined:

log: !Kafka
  # -- required

  # The name of the Kafka cluster to use for the source log.
  cluster: kafkaCluster

  # Name of the Kafka topic to use for the source log.
  # (Can be set as an !Env value)
  topic: "xtdb-log"

  # -- optional

  # The name of the Kafka cluster to use for the replica log (v2.2+).
  # Defaults to the same cluster as the source log.
  # replicaCluster: kafkaCluster

  # Name of the Kafka topic to use for the replica log (v2.2+).
  # Defaults to "${topic}-replica".
  # replicaTopic: "xtdb-log-replica"

  # Whether or not to automatically create the topics, if they do not already exist.
  # Applies to both the source and replica topics.
  # autoCreateTopic: true
```

### SASL Authenticated Kafka Example

The following piece of node configuration demonstrates the following common use case:

- Cluster is secured with SASL - authentication is required from the module.
- Topic has already been created manually.
- Configuration values are being passed in as environment variables.

``` yaml
remotes:
  kafkaCluster: !Kafka
    bootstrapServers: !Env KAFKA_BOOTSTRAP_SERVERS
    propertiesMap:
      sasl.mechanism: PLAIN
      security.protocol: SASL_SSL
      sasl.jaas.config: !Env KAFKA_SASL_JAAS_CONFIG

log: !Kafka
  cluster: kafkaCluster
  topic: !Env XTDB_LOG_TOPIC
  autoCreateTopic: false
```

The `KAFKA_SASL_JAAS_CONFIG` environment variable will likely contain a string similar to the following, and should be passed in as a secret value:

    org.apache.kafka.common.security.plain.PlainLoginModule required username="username" password="password";

## Leader election and fencing

The ['Database architecture'](/about/dbs-in-xtdb#database-architecture) page describes XTDB's single-writer indexing model in terms of properties — exactly one leader per database, automatic failover, followers as hot standbys.
This section describes how those properties are enforced.

### Election runs in the replica log

Each database's replica topic is a durable total order that every node reads, so leadership is settled in the topic itself rather than by anything outside it.

A node claims leadership by appending a no-op stamped one **term** above the highest it has read.
That claim confers leadership if, and only if, nothing at or above its term precedes it in the topic — so the claim's position in the log *is* the election result, and every node computes the same answer from the same prefix.
There is no coordinator, no vote, and nothing for an operator to configure.

A node claims when a poll of the replica topic comes back empty, meaning it looked and found the topic at its tip.
Timeouts are randomised, so two nodes that start claiming together rarely collide, and a claim that loses costs one wasted record.

Because each database elects independently, leaderships are no longer spread evenly across the cluster: one node may lead several databases while another leads none.

### Fencing by term

A leader applies and acknowledges a write only once it has read that write *back* from the replica topic at its own term, with no higher-term record ahead of it.

When leadership moves, the incoming leader writes at a higher term.
The outgoing leader reads that higher-term record back, recognises it has been superseded, and stands down — its own unconfirmed writes are never acknowledged.
Followers apply the highest term they have seen and discard lower-term records.
So at most one leader's writes are ever confirmed for a given database, even across an unclean handover, and without relying on Kafka transactions.

Terms come from the replica topic itself, which is the same durable record every reader is fenced against, so there is nothing to reset and nothing to keep in step with it.

### Sharing a Kafka cluster across deployments

Nothing outside a database's own topics takes part in electing its leader, so two XTDB deployments sharing a Kafka cluster are isolated by their topic names alone.
Give each deployment its own topics — and, if you want the separation enforced, its own ACLs.

## Kafka Log Durability

Kafka-backed logs offer strong durability, but require tuning and backup strategies to align with your recovery objectives.

### Recommended Kafka Settings

The replication factor, `min.insync.replicas` and `retention.ms` are the three that bear on data loss, and all three are yours rather than XTDB's — see [Settings](#settings).
Size `retention.ms` and `retention.bytes` so that unindexed messages survive long enough to be backed up or flushed.

See [Apache Kafka documentation](https://kafka.apache.org/documentation/) for details.

Managed services like [Confluent Cloud](https://www.confluent.io/confluent-cloud/) may offer higher guarantees and simplified observability.

### Strategies for Kafka Log Backup

There are three main ways to safeguard your XTDB Kafka log:

#### Point-in-Time Backups

:::caution
Always back up the storage module **before** backing up the log.
Restoring a log without its corresponding flushed storage state may result in inconsistency and force an epoch reset.

- Take backups **after** a successful XTDB storage flush.
- Capture **only committed** Kafka messages (exclude in-flight transactions).
- Use Kafka tooling or snapshotting scripts.
:::

#### Continuous Replication

Use Kafka-native tools to replicate log data between clusters:

- [MirrorMaker](https://kafka.apache.org/documentation/#basic_ops_mirror_maker)
- [Confluent Replicator](https://docs.confluent.io/platform/current/multi-dc-deployments/replicator/index.html)

This allows for:

- Geo-redundancy
- Low-RPO disaster recovery
- Hot-standby clusters

Note: Replication **does not** replace backups --- it only increases availability.

#### Application-Level Transaction Replay

XTDB can rebuild its state from upstream sources (event logs, message queues) used to submit transactions.

Advantages:

- Independent recovery source
- Replay can be filtered, transformed, or validated
- Fills gaps between backup and failure
