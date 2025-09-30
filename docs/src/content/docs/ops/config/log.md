---
title: Log
---

<details>
<summary>Changelog (last updated v2.1)</summary>

v2.1: multi-database support

: As part of the multi-database support, the Kafka log-clusters were extracted - see the [Kafka log documentation](log/kafka) for more details.

</details>

One of the key components of an XTDB node is the log - this is a totally ordered log of all operations that have been applied to the database, generally persistent & shared between nodes.

## Implementations

We offer a number of separate implementations of the log, currently:

- Single-node log implementations, within `xtdb-core`:
    - [In memory](#in-memory): transient in-memory log.
    - [Local disk](#local-disk): log using the local filesystem.
- [Remote](#remote): multi-node log implementations using a remote service.

## In memory

By default, the log is a transient, in-memory log:

``` yaml
## default, no need to explicitly specify

## log: !InMemory
```

If configured as an in-process node, you can also specify an [InstantSource](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/InstantSource.html) implementation - this is used to override the local machine's clock when providing a system-time timestamp for each message.

## Local disk

A single-node persistent log implementation that writes to a local directory.

``` yaml
log: !Local
  # -- required

  # The path to the local directory to store the log in.
  # (Can be set as an !Env value)
  path: /var/lib/xtdb/log

  # -- optional

  # The number of entries of the buffer to use when writing to the log.
  # bufferSize: 4096
```

If configured as an in-process node, you can also specify an [InstantSource](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/InstantSource.html) implementation - this is used to override the local machine's clock when providing a system-time timestamp for each transaction.

## Remote

A multi-node persistent log implementation that uses a remote service to store the log.

We currently offer the following remote log implementations, available in their own modules:

- [Kafka](log/kafka): a log implementation that uses a Apache Kafka topic to store the log.

## Epochs

An **epoch** is a manually assigned, monotonically increasing integer used to identify the generation of the log in XTDB:

- Epochs allow a cluster to safely reset its log state following partial log loss, corruption, or intentional recovery operations, without requiring full reindexing of storage data.
- If not explicitly configured, nodes assume `epoch = 0`.

### Configuration

To configure an epoch, specify the `epoch` field inside the node's log configuration:

``` yaml
log: !<LogType>
  epoch: <new-epoch>
```

Where:

- `<LogType>` is the chosen log implementation (e.g., `!Kafka`, `!Local`).
- `<new-epoch>` is a positive integer greater than the previous epoch.

All nodes within the same cluster **must** use an identical epoch value at startup.

#### Bumping an Epoch

:::caution
Beginning a new epoch will make the recovery of unindexed transactions from any previous epoch nearly impossible.
Therefore, it is recommended to only increment the epoch after you are confident that the original log is irrecoverable and you understand the potential consequences.

When applying a new epoch:

- Shut down all XTDB nodes to prevent divergence.
- Update each node's configuration with the new `epoch` value.
- (Optional) Prepare a clean log backend if required (e.g., create a new Kafka topic or clear the local log directory).
- Restart all nodes simultaneously with the updated configuration.

Once restarted, nodes will begin writing to the new log generation, and prior log history will be disregarded.

:::
