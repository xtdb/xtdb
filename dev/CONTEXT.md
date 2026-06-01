# CONTEXT

Business and product context for XTDB 2.x — what it is and who it serves.
Target state only; no technical detail, no change log.
Status: current.
The full product story lives in the root `README.adoc` and the end-user docs site (`docs/`); this is a short orientation.

## What XTDB is

XTDB is an immutable, bitemporal SQL database.
It records data on two independent time axes and never overwrites history, so any past state can be queried as-of any point in time.
It is built by JUXT and is open source.

## Core value

- **Bitemporal by default**: every row is versioned on **system time** (when XTDB recorded it) and **valid time** (when it is true in the domain).
  This gives time-travel queries, retroactive and scheduled corrections, and full audit history without bolt-on snapshots or backups.
- **Immutable / append-only**: data accumulates; the full history is always available.
- **SQL-first, Postgres-compatible**: speaks a SQL dialect with SQL:2011 temporal semantics over the PostgreSQL wire protocol, so existing Postgres clients, drivers and tools connect directly.
  XTQL is offered as an alternative, composable query language.
- **Separation of storage and compute**: durable state lives in an object store and a log; nodes are largely stateless and scale independently.
- **Cloud-native storage**: object-store backed (S3, Azure Blob, Google Cloud Storage, or local), with a Kafka (or local) transaction log.

## Who it's for

Applications where history and temporal correctness matter: compliance and regulatory retention, audit trails, finance, insurance, healthcare, and any domain that must answer "what did we know, and when did we know it?".
Teams that want those guarantees while keeping a familiar SQL / Postgres surface.

## Shape of a deployment

A node connects to a log and an object store, exposes a Postgres-wire endpoint, and can host multiple independent databases.
A standalone Docker image runs everything in-process for local or single-node use; cloud images pair the engine with Kafka and the relevant object store.
