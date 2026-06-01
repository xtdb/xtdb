# ARCHITECTURE

Technical architecture of XTDB 2.x: the layering, the transaction/indexing model, and the query path.
Status: current.
This file is the grep-friendly orientation map; the authoritative narrative is `dev/doc/high-level-tour.adoc` and the `dev/doc/*.allium` specs — follow those for the full story and the exact behaviour.

## Layering: Storage / State / Services

XTDB is "functional core, imperative shell" expressed with OO vocabulary.
Dependencies flow inward: Services depend on State and Storage; State has no runtime I/O (it only hydrates from Storage at startup).

- **Storage** (I/O): the log and the object store.
- **State** (in-memory): catalogs and the live index — yield data, do no I/O.
- **Services** (orchestration): processors, compactor, garbage collector — do the I/O.

## Storage layer

- **Log** (Kafka or local), per database, two streams:
  - **source log** — clients submit transactions (many writers); also carries flush signals and compaction results.
  - **replica log** — the leader's resolved output (single writer); the source of truth that all nodes converge on.
- **Object store** (S3 / Azure Blob / GCS / local):
  - **L0 blocks** — batches of transaction events in Arrow format.
  - **L1+ tries** — compaction output, an LSM tree split into current (infinite-recency) and historical (finite-recency) sides.
  - **table blocks** — per-block, per-table metadata and row summaries.

## State layer (in-memory)

- **LiveIndex** (`indexer/LiveIndex.kt`) — in-flight transactions not yet persisted to a block; snapshotted per-tx for concurrent reads; rebuilt from the latest block + replica log on restart.
- **TrieCatalog** (`trie_catalog.clj`, `trie/`) — per-table inventory of immutable trie snapshots (nascent → live → garbage); current/historical split.
- **BlockCatalog** (`block_catalog.clj`) — latest persisted block per database.
- **TableCatalog** (`table_catalog.clj`) — per-table metadata.
- **Watchers** (`api/log/Watchers.kt`) — latest tx-id / source-msg-id, enabling read-your-writes; also carries the external-source resume token.

## Services layer: single-writer-per-database indexing

`LogProcessor` is a state machine driven by Kafka consumer-group rebalancing — Kafka does the leader election, XTDB does not.
Exactly one leader per database; a node may lead database A while following database B.

- **Leader** (`LeaderLogProcessor.kt`) — consumes the source log, resolves transactions (puts/deletes/erases, constraints), writes `ResolvedTx` to the replica log via a **fenced transactional producer**, feeds LiveIndex, and triggers block flush when LiveIndex fills or on a timeout (thresholds in `IndexerConfig`).
- **ExternalSourceLeader** (`ExternalSourceProcessor.kt`) — CDC-driven; ingests from an external system (e.g. Postgres logical replication), assigns its own tx-ids, rejects client `submitTx`, and carries a resume token.
- **Follower** (`FollowerLogProcessor.kt`) — consumes the replica log, applies `ResolvedTx` to LiveIndex; buffers across a block boundary until the block is uploaded.
- **Transition** (`TransitionLogProcessor.kt`) — ephemeral startup/handoff state that replays the dead leader's pending block and catches up the replica log before becoming leader.

Supporting services:
- **BlockUploader** (`BlockUploader.kt`) — freezes LiveIndex, registers L0 tries, writes table blocks and the block file to the object store, writes `BlockUploaded` to the replica log (fenced), advances LiveIndex, signals the compactor.
- **Compactor** (`compactor.clj`, `compaction.allium`) — runs on every node, coordination-free (deterministic job selection + idempotent output); merges small tries into larger LSM levels.
- **GarbageCollector** (`garbage_collector.clj`, `gc.allium`) — deletes superseded tries from the object store and the TrieCatalog.
- **BufferPool** (`storage/BufferPool.kt`, `RemoteBufferPool.kt`) — memory-mapped, locally-cached access to object-store files.

## Transaction lifecycle

```
client submitTx → source log → Leader resolves → replica log (fenced)
   → LiveIndex (snapshots for reads) → BlockUploader (on full/timeout)
   → L0 block + tries in object store → Compactor merges L0→L1→L2+
   → query scan merges LiveIndex + tries for the requested VT/ST
```

## Query path

1. **Parse** — SQL via ANTLR (`core/src/main/antlr/`, `sql/parse.clj`); XTQL via hand-rolled parser (`xtql/`).
   Both lower to one relational form.
2. **Plan & optimise** — `logical_plan.clj`: relational algebra (`:scan`, `:select`, `:project`, `:join`, `:group-by`, `:order-by`, …) with equivalence rewrites (filter push-down, subquery decorrelation) applied to fixpoint.
3. **Execute** — pull-based operator pipeline (`operator/`); expressions compiled to vectorised kernels (`expression.clj`).
   The **scan** operator (`operator/scan/ScanCursor.kt`) is the heart: it selects LSM files via TrieCatalog metadata (Bloom filters on IID, temporal bounds), merge-sorts pages from LiveIndex + tries, and performs **bitemporal resolution** so downstream operators see a materialised valid-time snapshot.

## Surfaces

- **pgwire** (`pgwire/`) — primary entry point; Postgres-wire compatible; multi-database via connection params.
- **Flight SQL** (`flight_sql/FlightSql.kt`).
- **In-process API** (`api/`) and a JDBC path.

## Multi-database

One node hosts multiple independent databases; each has its own source + replica log topics and its own elected leader.

## Testing architecture

Test tasks, tags, locations and namespace-filter conventions are documented authoritatively in `AGENTS.md` ("Running tests") and `dev/README.adoc` ("Testing") — not repeated here.
Architecturally relevant points: property-based testing uses `clojure.test.check`; integration uses Testcontainers; and the `*.allium` specs encode domain behaviour and are kept in sync with code as part of the Definition of Done.

## Cross-cutting

- **Arrow** is the in-memory and on-disk columnar format throughout (off-heap via Netty allocator).
- **Errors**: `xtdb.error` / `Anomaly` sealed hierarchy — see `AGENTS.md` ("Errors") and `api/.../xtdb/error.clj` for the categories.
- **Serialization**: Protobuf for block/log wire formats; Transit for Clojure data.
- **Observability**: Micrometer + OpenTelemetry; Prometheus/CloudWatch/Azure Monitor registries.
