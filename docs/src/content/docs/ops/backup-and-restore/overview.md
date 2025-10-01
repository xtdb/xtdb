---
title: Backup and Restore
---

This document provides a high-level overview of XTDB's stateful architecture and outlines what data is stored where, what typical backups include, and how to think about restoring XTDB in different failure scenarios.

## Overview of Stateful Components

XTDB consists of two primary stateful components:

### Log

The log contains the record of all submitted transactions and inter-node messages.

- The log is structured as a totally ordered, append-only sequence of entries.
- Each transaction is recorded immutably and assigned a durable log offset.
- The log guarantees ACID-compliant, serial execution of all writes across the cluster.

From a backup perspective, the log is the source of truth for all changes made to the database.
Losing log entries that have not yet been indexed into storage risks permanent data loss.

For more information on the available implementations of the transaction log, see the [Log](/ops/config/log) docs.

### Storage Module

The storage module contains the database's state: tables, indexes, and supporting metadata.

- Storage consists of immutable, append-only files.
- Each file represents a snapshot of the database's state at a specific point in the transaction log.

From a backup perspective, storage files can be treated as **immutable snapshots** aligned with specific log offsets, making them well-suited to incremental or full backup strategies.

For more information on available storage backends and configuration, see the [Storage Module](/ops/config/storage) docs.

## What Data Is at Risk?

| Component | Risk if Lost |
| --- | --- |
| Log | All transactions on the log since the last indexed offset would be lost. |
| Storage Module | Without a backup or infinite retention on the log, all indexed data would be lost. |
| XTDB compute nodes | Only transient, in-memory cache state would be lost. All critical data is durably recorded in the log and storage module. Compute nodes can be restarted or replaced without risking data loss. |


## Backup Goals

Backup strategies in XTDB support multiple operational and business objectives:

- **Disaster recovery** --- restoring service after infrastructure or cloud failures
- **Point-in-time recovery of the individual storage components** - reverting storage components to a previous known good state
- **Environment migration** --- moving data between clusters, regions, or cloud providers

The specific backup approach should be chosen based on the criticality of the data, recovery time objectives (RTO), and regulatory requirements.

## What To Back Up?

For most deployments, backing up the **storage module** is essential.
It represents the finalized state of the database and enables full restoration without relying on the log.

Backing up the **log** is optional and may be used in conjunction with other strategies (e.g. application-level replay of transactions).

## Backing Up the Storage Module

XTDB storage is composed of immutable files aligned with flushed log blocks, making it ideally suited for full snapshot-style backups.

### Safeguarding Object Store Data

While cloud object stores such as **S3**, **Azure Blob Storage**, and **Google Cloud Storage** offer strong durability, they do not protect against:

- Accidental or malicious deletion
- Misconfigured lifecycle policies or IAM roles
- Loss of access due to control-plane issues

To mitigate these risks:

- **Enable versioning** --- to recover deleted or overwritten files
- **Enable soft delete / retention policies** --- to guard against accidental loss
- **Use cross-region replication** --- for geo-resiliency and DR readiness

For provider-specific recommendations, see:

- [Protecting XTDB Data (AWS)](/ops/aws#protecting-data)
- [Protecting XTDB Data (Azure)](/ops/azure#protecting-data)
- [Protecting XTDB Data (GCP)](/ops/google-cloud#protecting-data)

### Taking Full Backups

XTDB storage files are written immutably and aligned with flushed blocks from the log.
This makes them ideally suited to snapshot-based backup strategies.

To perform a full backup:

- Capture the full object store prefix or container used by XTDB
- Ensure all files associated with the latest indexed block are included
- Avoid partial or in-progress files --- only finalized files are valid for recovery

For platform-specific guidance, refer to:

- [Backing Up XTDB Data (AWS)](/ops/aws#backup)
- [Backing Up XTDB Data (Azure)](/ops/azure#backup)
- [Backing Up XTDB Data (GCP)](/ops/google-cloud#backup)

## Backing Up the Log

While the storage module captures finalized state, the log may contain recent transactions that have not yet been indexed.
If this data is lost before being written to storage, it cannot be recovered.

Backing up the log is optional, but may reduce your Recovery Point Objective (RPO) in the event of failure.

### Safeguarding Log Data

For advice on safeguarding the contents of the log, see implementation-specific guidance:

- [Kafka Log Durability](/ops/config/log/kafka#durability)

### Why Back Up the Log?

Backing up the log is not strictly required for all XTDB deployments.
In many cases, a full recovery can be achieved using the storage module alone.

This is because XTDB periodically flushes indexed transactions from the log into immutable storage files.
As a result, the storage module acts as a form of backup for the log --- capturing the system state at known log offsets.

Flush behavior is controlled by:

- A threshold number of transactions.
- A maximum time interval between flushes (this defaults to 4 hours)

These settings define your effective **Recovery Point Objective (RPO)** --- that is, how much recent data you could lose in the event of log failure.

However, backing up the log provides additional benefits in environments with stricter recovery requirements:

- **Recover transactions submitted after the last flush** --- reduces data loss compared to storage-only restores
- **Avoid resetting the `epoch`** --- restoring the log preserves continuity, allowing nodes to recover without configuration changes
- **Faster point-in-time recovery** --- restoring log and storage together may reduce bootstrap time and operational complexity

### How to Back Up the Log

Log protection can be achieved through:

- **Point-in-time backups** --- taken **after** a successful storage flush
- **Log replication** --- continuously replicating the log to another system or region
- **Application-level replay** --- rebuilding state from upstream message queues or events

:::caution
When taking point-in-time backups - **always** back up the **storage module first**, then the log.
Backing up the log before its associated storage state can result in a mismatch, as the restored log may refer to transactions not yet flushed to storage.
In this case, XTDB will require a reset of the `epoch`, effectively discarding the restored log and falling back to the storage backup alone --- with a corresponding loss of recent transactions.

Additionally, ensure the delay between the storage and log backups does **not** exceed the retention period of your log implementation.
If log messages are expired before the backup runs, they will be lost and cannot be restored.
:::

For implementation-specific instructions, refer to:

- [Strategies for Kafka Log Backup](/ops/config/log/kafka#backup) 

## Failure and Recovery Scenarios

The following are common failure scenarios and links to detailed guidance for each:

- [**Transaction log is out of sync**](out-of-sync-log)
