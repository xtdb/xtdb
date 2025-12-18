# Glossary

## User-facing concepts

See the [XTDB docs](https://docs.xtdb.com):

| Term | Docs |
| --- | --- |
| bitemporality | [Time in XTDB](https://docs.xtdb.com/about/time-in-xtdb) |
| system time | [Time in XTDB - System time](https://docs.xtdb.com/about/time-in-xtdb#system-time) |
| valid time | [Time in XTDB - Valid time](https://docs.xtdb.com/about/time-in-xtdb#valid-time) |
| document / row | [Key concepts - Records & Rows](https://docs.xtdb.com/concepts/key-concepts#records--rows) |
| entity / record | [Key concepts - Records & Rows](https://docs.xtdb.com/concepts/key-concepts#records--rows) |
| transaction | [Transactions in XTDB](https://docs.xtdb.com/about/txs-in-xtdb) |
| await token | [Transactions - Transaction consistency](https://docs.xtdb.com/about/txs-in-xtdb#transaction-consistency) |
| snapshot token | [Transactions - Snapshots](https://docs.xtdb.com/about/txs-in-xtdb#snapshots) |
| basis | [Transactions - Repeatable queries](https://docs.xtdb.com/about/txs-in-xtdb#repeatable-queries---basis) |
| log | [Log configuration](https://docs.xtdb.com/ops/config/log) |
| object-store | [Storage configuration](https://docs.xtdb.com/ops/config/storage) |

## Components

See [`dev/doc/high-level-tour.adoc`](doc/high-level-tour.adoc) for more detail. For the Integrant system, see `xtdb.node.impl/node-system` and `xtdb.db-catalog/db-system`.

| Term | Description |
| --- | --- |
| block catalog | Tracks which blocks exist in the object store and their metadata. |
| buffer pool | A pool of Arrow buffers, managed for reuse and reference counted. Fetches data from the object store. |
| compactor | Merges LSM tree files to pre-compute merge-sorts, so queries only read necessary data. L0→L1 coalesces small files to ~100MB; L1→L2+ partitions by IID bit-prefix with a branching factor of four. |
| db catalog | Manages the set of databases in a node, handling attach/detach operations. |
| expression engine (EE) | Compiles expressions into functions that the query engine can efficiently execute. Used for queries and metadata checking. |
| garbage collector | Removes files from the object store that are no longer referenced by the current state of the LSM tree. |
| indexer | Reads transactions from the log, evaluates them, and uploads blocks of rows as L0 files to the object store. |
| live index | In-memory index of transactions not yet written to a block. Included in queries so they can observe recent transactions. On startup, nodes check the latest block in the object store and consume the log from there. |
| log processor | Consumes messages from the log and dispatches them for indexing. |
| metadata manager | Manages block and page-level metadata used to skip irrelevant data during queries. |
| query source | Prepares and executes queries against the database. |
| table catalog | Tracks which tables exist and their schemas. |
| trie catalog | Manages the trie index structures used for lookups. |

## Internal concepts

| Term | Description |
| --- | --- |
| bitemporal resolution | The process by which the scan operator reduces a system-time descending list of events for each entity into the history relevant to the query. |
| block | A group of (~100k) rows that the indexer holds in the live index before flushing to disk as an L0 file in the LSM tree. |
| event | A put, delete or erase operation. Contains the IID, system-time, user-specified valid-time range, and document (for puts). System-to and effective valid-time range are derived through bitemporal resolution. |
| expression | A predicate, test or calculation in SQL/XTQL that acts on columns/variables. Compiled into Clojure 'kernels' for efficient vector execution. |
| golden stores | The log and the object-store - the two durable stores through which XTDB nodes communicate. |
| IID | A 16-byte internal identifier for an entity's primary key (`xt/id`). |
| logical plan | A relational algebra representation of a query or update, represented as a nested Clojure structure. The engine may apply optimizations (e.g. semantically equivalent but more efficient rewrites). |
| LSM tree | Log-structured merge tree - the primary index structure. L0 contains block files from the indexer, L1 coalesces these to ~100MB, and L2+ partitions by IID bit-prefix. |
| metadata | Per-block and per-page data that allows quickly ruling out blocks/pages that don't satisfy certain conditions (temporal or content related). |
| operator | A relational algebra operator. Source operators (`:scan`, `:table`), intermediate operators (`:select`, `:project`, `:join`, etc.). |
| page | A group of (~1024) rows. We process rows in page batches to amortize the overhead of parsing data, setting up copiers, etc. |
| snapshot | An immutable snapshot of the live index state, taken after every transaction, allowing queries to read concurrently while the indexer continues. |
