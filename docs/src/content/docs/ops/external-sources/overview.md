---
title: External Sources
---

A secondary database can be backed by an external source.
Rather than accepting transactions from clients, it subscribes to an upstream feed and records what it reads as XTDB transactions.

The database is then a read-only mirror of that upstream — it rejects writes from the [Postgres wire-compatible server](/ops/config#postgres-wire-compatible-server) and the [Flight SQL server](/ops/config#flight-sql-server), and tracks the upstream as it changes.

## Supported sources

Postgres

: Change-data-capture from a Postgres database — see [Postgres External Source](/ops/external-sources/postgres/setup).

Kafka Connect

: Records from a Kafka Connect topic — see [Kafka Connect External Source](/ops/external-sources/kafka-connect/setup).

## Configuring an external source

An external source is set up when you [attach the secondary database](/about/dbs-in-xtdb#attachingdetaching-secondary-databases-v21): add an `externalSource:` entry to the `ATTACH DATABASE` config, alongside its `log:` and `storage:`.

See each source's setup guide for the specifics.

## Ingest-only nodes

External sources can also run on a dedicated ingest-only node — a node with no query surface, useful for scaling ingestion independently of the nodes serving queries.
See [Ingest-only nodes](/ops/config#ingest-only-nodes-v22).
