---
title: Kafka
---

<details>
<summary>Changelog (last updated v2.1)</summary>

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

[Apache Kafka](https://kafka.apache.org/) can be used as an XTDB message log.

## Setup

1. Add a dependency to the `com.xtdb/xtdb-kafka` module in your dependency manager.
2. On your Kafka cluster, XTDB requires a Kafka topic to use as its message log:
    - This can be created manually and provided to the node config, or XTDB can create it automatically.
    - If allowing XTDB to create the topic **automatically**, ensure that the connection properties supplied to the XTDB node have the appropriate permissions to create topics - XTDB will create the topic with some expected configuration values.
3. The log topic should be configured with the following properties:
    - **A single partition** - this ensures that the message log is strictly ordered.
    - `message.timestamp.type` set to `LogAppendTime` - this ensures that the timestamp of the message is the time it was appended to the log, rather than the time it was sent by the producer.
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

4. XTDB should be configured to use the log topic, and the Kafka cluster the topic is hosted on.
  It should also be authorised to perform all of the necessary operations the topic.
    - For configuring the kafka module to authenticate with the Kafka cluster, the `propertiesFile` or `propertiesMap` configuration options to supply the necessary connection properties.
      See the [example configuration](#auth_example) below.
    - If using the Kafka cluster is using **ACLs**, ensure that the XTDB node has the following permissions on the topic:
        - `Describe`
        - `Read`
        - `Write`

## Configuration

To use the Kafka module, include the following in your node configuration:

``` yaml
## We first declare the Kafka log cluster:

logClusters:
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

## For the database, we then create a log using the Kafka cluster we just defined:

log: !Kafka
  # -- required

  # The name of the Kafka cluster to use for the primary log.
  cluster: kafkaCluster

  # Name of the Kafka topic to use for the primary log.
  # (Can be set as an !Env value)
  topic: "xtdb-log"

  # -- optional

  # Whether or not to automatically create the topic, if it does not already exist.
  # autoCreateTopic: true
```

### SASL Authenticated Kafka Example

The following piece of node configuration demonstrates the following common use case:

- Cluster is secured with SASL - authentication is required from the module.
- Topic has already been created manually.
- Configuration values are being passed in as environment variables.

``` yaml
logClusters:
  kafkaCluster: !Kafka
    bootstrapServers: !Env KAFKA_BOOTSTRAP_SERVERS
    propertiesMap:
      sasl.mechanism: PLAIN
      security.protocol: SASL_SSL
      sasl.jaas.config: !Env KAFKA_SASL_JAAS_CONFIG

log: !Kafka
  topic: !Env XTDB_LOG_TOPIC
  autoCreateTopic: false
```

The `KAFKA_SASL_JAAS_CONFIG` environment variable will likely contain a string similar to the following, and should be passed in as a secret value:

    org.apache.kafka.common.security.plain.PlainLoginModule required username="username" password="password";

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
