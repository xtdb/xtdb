---
title: Configuration
---

<details>
<summary>Changelog (last updated v2.2)</summary>

v2.2: `remotes` replaces `logClusters`

: Named connections to external systems are now configured under [`remotes`](#remotes).

  Previously these lived under `logClusters`, which was scoped to transaction-log clusters. `remotes` generalises it to any external connection — Kafka clusters, cloud identities, Postgres databases, etc.

  `logClusters` is deprecated but still honoured: entries under both names are merged, so existing config keeps working — rename to `remotes` when convenient.

  The pre-existing `!Kafka` can simply be moved under `remotes` with no other changes.

v2.1: multi-database support

: The log and storage configurations were changed as part of 2.1's multi-db support.

  For more details on those changes, see the [Transaction Logs](config/log) and [Object Storage](config/storage) documentation.

</details>

XTDB nodes are configured using YAML files.

All config options have default values, it is therefore valid to not specify a config file or not specify any part of the top-level config.

## Log & Storage

The two main pluggable components of XTDB are [transaction logs](config/log) and [object storage](config/storage).

``` yaml
## transaction log configuration
log: !Local
  path: /path/to/log-file

## object store configuration
storage: !Local
  path: /path/to/storage-dir
```

By default XTDB will use an [in-memory transaction log](config/log#in-memory) and an [in-memory object store](config/storage#in-memory).

## Monitoring & Observability

XTDB provides a suite of tools & templates to facilitate [Monitoring & Observability](config/monitoring).

By default [healthz](config/monitoring#healthz-server), [monitoring](config/monitoring#metrics) & [tracing](config/monitoring#tracing) are disabled.

## Authentication

The Postgres wire-compatible server supports [authentication](config/authentication) which can be configured via authentication rules.

By default a [single root user](config/authentication#single-root-user-v22) named `xtdb` accepting any password is configured.

## Caching

XTDB has two caches for [object store](config/storage) data:

```yaml
# By default configured to use half the JVM's maximum direct memory
memoryCache:
  # Maximum size of the cache, in bytes.
  # Defaults to being calculated via `maxSizeRatio`
  maxSizeBytes: 1073741824

  # Maximum size of the cache, as a proportion of the JVM's maximum direct memory.
  # Ignored when `maxSizeBytes` is set.
  maxSizeRatio: 0.5 # default

# Required when using a remote object store, otherwise unused
# Defaults to being disabled
diskCache:
  # Directory in which to store cached files. Required.
  path: /path/to/disk-cache

  # Maximum size of the cache, in bytes.
  # Defaults to being calculated via `maxSizeRatio`
  maxSizeBytes: 10737418240

  # Maximum size of the cache, as a proportion of the filesystem's total space.
  # Ignored when `maxSizeBytes` is set.
  maxSizeRatio: 0.75 # default
```

## Remotes

`remotes` is a registry of named connections to the external systems XTDB authenticates against — Postgres instances, Kafka clusters, cloud identities (AWS/Azure/GCP), etc.

You define a connection once here under an alias, then reference it by that alias from the parts of the config that use it — the [transaction log](config/log), [object storage](config/storage), and external sources.

Each entry maps an alias to a connection. The tag (`!Postgres`, `!Kafka`, ...) selects the remote type, and the available fields depend on that type:

```yaml
remotes:
  my-kafka: !Kafka
    bootstrapServers: "localhost:9092"

# ...then referenced by alias, e.g. by a Kafka transaction log:
log: !Kafka
  cluster: my-kafka
  topic: xtdb_topic
```

For the fields each remote type takes, and how a given component references its alias, see that component's documentation — e.g. [transaction log](config/log), [object storage](config/storage).

## Troubleshooting configuration

Config options to help an operator troubleshoot XTDB.

### Read-only Databases

Set all databases in the cluster to be in read-only mode.

By default set to false to have [databases](/about/dbs-in-xtdb) run in their configured modes.

```yaml
## Set to true to have *all* databases run in read-only mode
readOnlyDatabases: true
```

### Skip Databases

Set the configured databases as dormant (queriable but not accepting transactions).

By default an empty list or the result of splitting `!Env XTDB_SKIP_DBS` by `,`.

```yaml
skipDbs:
  - my_db
  - my_other_db
```

## Other configuration

### Postgres wire-compatible server

By default read-write Postgres wire-compatible server is started on localhost:5432.

``` yaml
server:
  # Host on which to start a read-write Postgres wire-compatible server.
  #
  # Default is "localhost", which means the server will only accept connections on the loopback interface.
  # Set to '*' to accept connections on all interfaces.
  host: localhost

  # Port on which to start a read-write Postgres wire-compatible server.
  #
  # Default is 0, to have the server choose an available port.
  # (In the XTDB Docker images, this is defaulted to 5432.)
  # Set to -1 to not start a read-write server.
  port: 0

  # Port on which to start a read-only Postgres wire-compatible server.
  #
  # The server on this port will reject any attempted DML/DDL,
  # regardless of whether the user would otherwise have the permission to do so.
  #
  # Default is -1, to not start a read-only server.
  # Set to 0 to have the server choose an available port.
  readOnlyPort: -1
```

### Flight SQL Server

By default a Flight SQL server is started on localhost:9832.

``` yaml
flightSql:
  # Host on which to start the Flight SQL server.
  #
  # Default is "127.0.0.1", which means the server will only accept connections on the loopback interface.
  # Set to '*' to accept connections on all interfaces.
  host: 127.0.0.1

  # Port on which to start the FLight SQL server.
  #
  # Default is 0, to have the server choose an available port.
  # (In the XTDB Docker images, this is defaulted to 9832.)
  # Set to -1 to not start a Flight SQL server.
  port: 0
```

### Compactor

Defaults to running on roughly half the number of threads the system has processor cores.

```yaml
compactor:
  # Number of threads to use for compaction.

  # Defaults to !Env XTDB_COMPACTOR_THREADS # or if that's not specified min(availableProcessors / 2, 1).
  # Set to 0 to disable the compactor.
  threads: 4
```

### Indexer

Responsible for indexing transactions from the [transaction log](config/log).

Please consider reaching out at hello@xtdb.com if you feel the need to change any of these!

```yaml
indexer:
  # Set to false to disable indexing on the primary database (xtdb).
  #
  # Transactions are still accepted onto the log but are never processed,
  # so synchronous submits will hang waiting for a result that never arrives.
  # Submit with `async=true` to not hang.
  enabled: true # default

  # Number of operations the in-memory live-index buffers before reorganising them.
  # Low-level tuning, most deployments leave this alone.
  logLimit: 64 # default

  # The maximum size of a page in the in-memory live-index.
  # Low-level tuning, most deployments leave this alone.
  pageLimit: 1024 # default

  # Number of operations the in-memory live-index buffers before flushing to the object store.
  # Low-level tuning, most deployments leave this alone.
  rowsPerBlock: 102400 # default

  # ISO-8601 duration after which the current block is finished even if it
  # hasn't reached `rowsPerBlock`
  flushDuration: PT4H # default

  # Transaction ids to skip during indexing
  # Useful to work around a transaction that crashes the indexer.
  #
  # Applies to *all* databases on the node.
  #
  # Defaults to the `XTDB_SKIP_TXS` environment variable (a comma-separated list
  # of transaction ids, e.g. "12,15,16") if set, otherwise empty.
  skipTxs: []
```

### Garbage Collector

Reclaims object-store space by deleting files left behind once compaction has superseded them.

Disabled by default.

```yaml
garbageCollector:
  # Set to true to enable garbage collection
  enabled: false # default

  # Number of recent blocks to retain
  blocksToKeep: 10 # default

  # ISO-8601 duration for which superseded trie files are retained
  garbageLifetime: PT24H # default
```

### Node ID

An identifier for the node. For example, used in [metrics](config/monitoring#metrics) and crash logging.

Defaults to `!Env XTDB_NODE_ID` otherwise is a short random string.

### Default TZ

Defaults to UTC.

## Ingest-only nodes (v2.2+)

An ingest-only node runs [external sources](external-sources/overview) and nothing else.

It has no query surface - no Postgres wire-compatible server, no Flight SQL server - making it useful for moving ingestion onto dedicated compute, away from the nodes serving queries.

For each configured database, the node joins that database's leader election and runs its external source when elected leader.

Started with the [`ingest` CLI command](#cli-toolsflags) — e.g. `docker run xtdb/xtdb ingest -f /config/xtdb.yaml`, or `args: ["ingest", "-f", "/config/xtdb.yaml"]` in Kubernetes — it takes its own top-level config file.

Each entry under `databases:` takes the same config as [`ATTACH DATABASE`](/about/dbs-in-xtdb#attachingdetaching-secondary-databases-v21) — `log`, `storage`, `externalSource`:

```yaml
remotes:
  src_kafka: !Kafka
    bootstrapServers: "localhost:9092"

# The databases to ingest into, keyed by name.
databases:
  kc_orders:
    log: !Kafka
      cluster: src_kafka
      topic: kc-orders-log
    storage: !Remote
      objectStore: !S3
        bucket: my-bucket
        prefix: kc-orders
    externalSource: !KafkaConnect
      remote: src_kafka
      topic: orders
      indexer: !Docs
        table: orders

# Required when any database uses a remote object store
diskCache:
  path: /var/cache/xtdb

# Serves /metrics and the liveness/readiness probes
healthz:
  port: 8080
```

The config also accepts [`memoryCache`](#caching), [`indexer`](#indexer), [`healthz`](config/monitoring#healthz-server) and [node ID](#node-id), with the same semantics as the regular node config.

`xtdb` is reserved for the primary database and can't be used as a database name here.

## Additional Concepts

### Using `!Env`

For certain keys, we allow the use of environment variables - typically, the keys where we allow this are things that may change **location** across environments.
Generally, they are either "paths" or "strings".

When specifying a key, you can use the `!Env` tag to reference an environment variable.
As an example:

``` yaml
storage: !Local
  path: !Env XTDB_STORAGE_PATH
```

Any key that we allow the use of `!Env` will be documented as such.

### CLI tools/flags

<details>
<summary>Changelog (last updated v2.1)</summary>

v2.1: top-level commands

: In v2.1, we changed the CLI to use top-level commands (not dissimilar to Git, for example).

  Previously, the playground and compact-only nodes were activated using optional flags - `--playground-port` and `--compact-only` respectively.

  `reset-compactor` and `export-snapshot` were also added in v2.1.
    
</details>

You can run various tools by passing arguments - either directly to the CLI or via Docker's arguments:

`node` (default, can be omitted)

: - `-f <file>`, `--file <file>`: specifies the configuration file to use.

`playground`

: Starts a playground - an in-memory server that will accept any database name, creating it if required.
  - `-p <port>`, `--port <port>` (default 5432): specifies the port to run the playground server on.

`compactor`

: Starts a compactor-only node - useful for giving the compaction process more compute resources.
  - `-f <file>`, `--file <file>`: specifies the configuration file to use.

`ingest` (v2.2+)

: Starts an [ingest-only node](#ingest-only-nodes-v22) - runs the configured external sources, with no query surface.
  - `-f <file>`, `--file <file>`: specifies the configuration file to use.

`reset-compactor <db-name>`

: Resets the compaction back to L0, deleting any L1+ files - use this if you've encountered a compaction bug and need to reset its state.

    1. Spin down all of your XT nodes
    2. Using your container orchestration tool (e.g.
      Kubernetes), run a one-shot task with an overriden command: `["reset-compactor"]`.
    Optionally, specify `--dry-run` to list all of the files to be removed.

    3. When the tool has finished, spin up your nodes again.

    You may want to also spin up a compactor-only node to help out
    with the re-compaction.

    At the moment, this can only reset all the way back to L0 -
    finer-grained reset will be added in a later release.
    
`export-snapshot <db-name>`

: - `-f <file>`, `--file <file>`: specifies the configuration file to use.

  This exports a snapshot of the object-store into a sibling directory within the object store.
  e.g. if your storage is at `s3://my-bucket/`, this will export to a directory under `s3://my-bucket/exports/...` - the exact directory will be given in the logs.
  
  You can then start another node against this storage directory - you will need to start a new log, and increase the log epoch in your configuration: 
  
  ```yaml
  log: !Kafka
    ...
    topic: new-topic
    epoch: 1
  storage: !Remote
    objectStore: !S3
      bucket: my-bucket
      prefix: exports/...
  ```

`read-arrow-file <file>`

: reads an Arrow file and emits it as EDN

`read-arrow-stream-file <file>`

: reads an Arrow 'stream IPC format' file and emits it as EDN

e.g.

- Dockerfile: `CMD ["playground", "--port", "5439"]`
- docker-compose: `command: ["playground", "--port", "5439"]`
- Java uberjar: `java -jar xtdb.jar playground --port 5439`
- Clojure (with `xtdb-core` in your `deps.edn`): `clj -M xtdb.main playground --port 5439`

You can also pass `--help` to any of the commands to get command-specific help.
