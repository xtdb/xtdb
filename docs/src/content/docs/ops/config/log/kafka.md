---
title: Kafka
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: single-writer support — two topics per database

: [Single-writer indexing](/about/dbs-in-xtdb#database-architecture) requires two Kafka topics per database: a **source log** for client writes and a **replica log** for the indexing leader's resolved output.
  XTDB uses Kafka's consumer-group rebalance protocol to elect the leader for each database, and fences split-brain writes to the replica log by term — see ['Leader election and fencing'](#leader-election-and-fencing) below.

  Previously, a database used a single Kafka topic, and every indexer node consumed it independently.
  With single-writer, only the elected leader consumes the source topic; followers tail the replica topic instead.

  Upgrading:

  - The replica topic defaults to `${topic}-replica` and auto-creates when `autoCreateTopic` is enabled, so existing deployments need no configuration changes to pick it up.
  - If multiple XTDB deployments share a Kafka cluster, give each a distinct `groupId` on its `!Kafka` cluster config — otherwise their consumer groups collide and leadership is assigned across deployment boundaries.
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
    # pollDuration: "PT1S"
    # propertiesFile: "kafka.properties"
    # propertiesMap:

  # became

  logClusters:
    kafkaCluster: !Kafka
  bootstrapServers: "localhost:9092"
      # pollDuration: "PT1S"
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
3. Both topics should be configured with the following properties:
    - **A single partition** — this ensures that the log is strictly ordered, and single-writer leader election (see [below](#leader-election-and-fencing)) relies on Kafka assigning the single partition to a single consumer at a time.
    - `message.timestamp.type` set to `LogAppendTime` — ensures the timestamp of the message is the time it was appended to the log, rather than the time it was sent by the producer.
    - The XTDB log is generally set up using the default values for configuration.
      A few key values to consider:
        - `retention.ms`: as messages are not required to be
    permanently on the log, this value does not need to be
    particularly high. The default value of **1 day** is
    sufficient for most use cases. When extra precaution should
    be taken to prevent any data loss on certain environments, a
    larger value is recommended, with **1 week** as a starting
    point.

        - `max.message.bytes`: generally, this is using the default
    value of **1MB**, which is fit for purpose for most log
    messages. This will depend on the overall size of
    transactions that are being sent into XTDB.

        - `cleanup.policy`: The Kafka module within XTDB does not make
    use of compacted messages, so it is recommended that the
    topic cleanup policy should use the default value of
            **delete**.

4. XTDB should be configured to use the topics, and the Kafka cluster they're hosted on.
  It should also be authorised to perform all of the necessary operations on both.
    - For configuring the Kafka module to authenticate with the Kafka cluster, use the `propertiesFile` or `propertiesMap` configuration options to supply the necessary connection properties.
      See the [example configuration](#auth_example) below.
    - If the Kafka cluster is using **ACLs**, the XTDB node needs:
        - `Describe` / `Read` / `Write` on **both** the source and replica topics.

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

    # -- optional
    # The maximum time to block waiting for records to be returned by the Kafka consumer.
    # pollDuration: "PT1S"

    # Path to a Java properties file containing Kafka connection properties,
    # supplied directly to the Kafka client.
    # (Can be set as an !Env value)
    # propertiesFile: "kafka.properties"

    # A map of Kafka connection properties, supplied directly to the Kafka client.
    # propertiesMap:

    # Consumer-group ID used for per-database leader election (v2.2+).
    # Defaults to "xtdb" — set a distinct value per deployment when multiple XTDB
    # deployments share a Kafka cluster, so their consumer groups don't collide.
    # groupId: "xtdb"

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

  # Declares that the consumer group backing leader election has been recreated (v2.2+).
  # Raise it — never lower it — when that happens; see 'Recreating the consumer group' below.
  # termEpoch: 0
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
This section describes how the Kafka module actually enforces those properties.

### Leader election via consumer groups

Every XTDB node running the Kafka log subscribes to each database's source topic via a shared Kafka consumer group (`groupId`, defaulting to `"xtdb"`).
Kafka's rebalance protocol then assigns each topic's single partition to exactly one consumer across the group — that consumer is the leader for that database.
When a leader node stops (crash, network partition, long GC pause) or a new node joins the group, Kafka triggers a rebalance and the assignment moves.

Because all databases on a node share one consumer group via a single underlying consumer, Kafka's `CooperativeStickyAssignor` distributes leaderships evenly across the cluster — e.g. with three nodes serving three databases, each node ends up leader for one database and follower for the other two.

### Fencing by term

Each leader stamps every record it writes to the replica log with a **term** — the generation number Kafka assigns its consumer-group membership, which strictly increases each time the assignment moves.
A leader applies and acknowledges a write only once it has read that write *back* from the replica log at its own term, with no higher-term record ahead of it.

If a rebalance has meanwhile moved leadership on, the incoming leader writes at a higher term.
The outgoing leader reads that higher-term record back, recognises it has been superseded, and resigns — its own unconfirmed writes are never acknowledged.
Followers apply the highest term they have seen and discard lower-term records.
So at most one leader's writes are ever confirmed for a given database, even across an unclean handover — without relying on Kafka transactions.

### Recreating the consumer group (v2.2+)

A term is not that generation number alone: it pairs it with a `termEpoch`, which orders one incarnation of the consumer group against the next.
The generation orders elections *within* one incarnation; the epoch is what survives the group being recreated.

`generationId` restarts at 1 whenever the group is recreated, below the terms already on the replica log.
The broker deletes a consumer group once it has no members left and its committed offsets have expired, so how long a group survives with every XTDB node down is governed by the broker's `offsets.retention.minutes` — seven days by default.
This turns on *members*, not traffic: a cluster that is up with no writes flowing keeps its group indefinitely, however long it idles.

Raise `offsets.retention.minutes` on the broker if you plan outages longer than that.
Raise `termEpoch` on the log config whenever the group is recreated anyway — an outage past the retention period, a `groupId` change, or a group you delete deliberately:

``` yaml
log: !Kafka
  cluster: kafkaCluster
  topic: "xtdb-log"
  termEpoch: 1
```

A node whose term is already fenced refuses to lead rather than indexing into a log every reader ignores, and its error names the current epoch:

```
leader term 0.1 is already fenced by 0.9 on the replica log — the leader-election
counter has regressed (a recreated Kafka consumer group, or a restarted local log),
so bump the log's termEpoch above 0
```

Raise `termEpoch`, never lower it: a lower value puts the new leader back below the terms on the log.

### Sharing a Kafka cluster across deployments

Leader election runs through a Kafka consumer group (`groupId`, defaulting to `"xtdb"`).
If you run multiple XTDB deployments against the same Kafka cluster (e.g. staging + prod, or multiple tenants), give each a distinct `groupId` — otherwise they join the same group and Kafka assigns their topics' partitions across both deployments' nodes.

``` yaml
remotes:
  kafkaCluster: !Kafka
    bootstrapServers: "localhost:9092"
    groupId: "prod"
```

## Kafka Log Durability

Kafka-backed logs offer strong durability, but require tuning and backup strategies to align with your recovery objectives.

### Recommended Kafka Settings

To minimize the risk of data loss:

- **Replicate the topic** - set a replication factor of `3+` for fault tolerance
- **Enforce quorum writes** - use `min.insync.replicas > 1`
- **Tune retention** - ensure `retention.ms` and/or `retention.bytes` keep unindexed messages long enough to allow for safe backup or flushing

XTDB sets safe producer defaults, but you must verify your topic-level configs.

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
