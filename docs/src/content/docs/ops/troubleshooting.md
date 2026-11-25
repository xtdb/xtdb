---
title: Troubleshooting
---

<details>
<summary>Changelog (last updated v2.2.1)</summary>

v2.2.1: the SQL parser cache is bounded

: The SQL parser's prediction cache holds at most 50,000 states by default, and is reset when it passes that — see [SQL parser cache resets](#sql-parser-cache-resets).

  Previously the cache was never evicted, so a workload whose SQL kept varying in structure could grow it until the node ran out of heap.

  No upgrade steps are required; set `XTDB_SQL_PARSER_CACHE_MAX_STATES=-1` to keep the previous unbounded behaviour.

</details>

JUXT provide [enterprise XTDB support](https://xtdb.com/support) for companies looking to adopt or extend their usage of XTDB, including training, consultancy and production support - email <hello@xtdb.com> for more information.

Given XTDB is a public, open-source project, you may also find useful information: - on [Discuss XTDB](https://discuss.xtdb.com) (public forum) - on the [GitHub repository](https://github.com/xtdb/xtdb) (where you can [check existing issues](https://github.com/xtdb/xtdb/issues) or [raise a new issue](https://github.com/xtdb/xtdb/issues/new).)

## Setting the logging level

The simplest way to modify XTDB's logging level is through environment variables:

- `XTDB_LOGGING_LEVEL`:: sets the logging level for all XTDB components.
- You can also set the logging level for individual components - for example:
- `XTDB_LOGGING_LEVEL_COMPACTOR`
- `XTDB_LOGGING_LEVEL_INDEXER`
- `XTDB_LOGGING_LEVEL_PGWIRE`

Valid levels are `TRACE`, `DEBUG`, `INFO` (default), `WARN`, and `ERROR`.

### JSON log output

Set `XTDB_LOG_FORMAT=json` to enable structured JSON log output, suitable for cloud log aggregation services.

`docker run -e XTDB_LOG_FORMAT=json …​`

### Custom logging configuration

For more control, you can supply a custom [Log4j2 configuration file](https://logging.apache.org/log4j/2.x/manual/configuration.html) to the Docker container using the following command:

`docker run --volume .:/config --env JDK_JAVA_OPTIONS='-Dlog4j2.configurationFile=/config/log4j2.xml' …​`

## Ingestion stopped

If you encounter an 'ingestion stopped' error, it means that XTDB has encountered an unrecoverable error while processing a transaction, and has stopped further transaction processing in order to prevent corruption to your data.
At this point, the built-in health checks will flag that the node is unhealthy and, if you're running XTDB within a container orchestrator (e.g. Kubernetes), it will restart the problematic node.

XTDB uses a single-writer indexing model: for each database, at any given time one node in the cluster (the leader for that database) processes the source log and writes indexed blocks to the object store; other nodes follow the leader's output via the replica log.
For deterministic errors - e.g. syntax errors, or divide by 0 - the leader rolls back the transaction in question and continues, and the other nodes observe the same outcome.
If a non-deterministic error is raised during indexing (e.g. the leader loses connection to the log or object-stores), that node cannot unilaterally make the decision to roll back the transaction and continue, as doing so could diverge from what's already been durably committed.

For temporary issues (e.g. connectivity), restarting the affected node will often resolve the issue without any manual intervention required - another node takes over as leader and indexing resumes.
However, in the rare event of an XTDB bug where we cannot guarantee that a subsequent leader wouldn't hit the same error, we choose to err on the side of safety, which may cause a restart loop.

In this case, XTDB will upload a crash log to your object-store containing the diagnostic information required to debug the issue - the exact location will be logged in your XTDB container.

Please do raise this with the XTDB team at <hello@xtdb.com>.

### Skipping Transactions

You *may* want to try skipping the errant transaction.

This action *must* be applied atomically: all nodes must be stopped, the configuration change applied, and then all nodes restarted. (Otherwise, the above risk applies - some nodes commit the transaction and others choose to roll it back.)

1. Verify that ingestion stops at the same transaction on all nodes, and identify the errant transaction ID in the logs.
2. Scale down all nodes within the XTDB cluster.
3. Set `XTDB_SKIP_TXS="<txId>,<txId2>…​"` as an environment variable on all of the nodes.
4. Restart all nodes within the XTDB cluster.

When the errant transaction has been skipped, you will see a log message: "skipping transaction offset 2109".
Once the next block has been written, you will then see another log message: "it is safe to remove the XTDB_SKIP_TXS environment variable".
This can be applied as a standard (green/blue) configuration change, at your convenience.

### Skipping Databases (v2.2+)

On startup, XTDB attaches every secondary database listed in the primary database's block catalog.
If one of those databases' underlying log (e.g. a Kafka topic) has been deleted or is otherwise unavailable, the attach fails and the node won't start — even if the rest of the cluster's state is healthy.

You can tell XTDB to skip specific secondary databases at startup using `XTDB_SKIP_DBS`.
Skipped databases become **dormant**: their configuration is preserved in the block catalog (so it survives future block writes and can be recovered later), but no processing starts.
Dormant databases are excluded from the list of active databases, so queries, the information schema, and the `healthz` endpoint naturally ignore them — a dormant database won't cause health checks to report unhealthy.

This action *must* be applied atomically: all nodes must be stopped, the configuration change applied, and then all nodes restarted.

1. Identify the problematic database name in the startup error logs.
2. Scale down all nodes within the XTDB cluster.
3. Set `XTDB_SKIP_DBS="db1,db2"` as an environment variable on all of the nodes.
4. Restart all nodes within the XTDB cluster.

Once the nodes have started, you have two recovery paths:

- **Fix the underlying issue** (e.g. recreate the Kafka topic), remove `XTDB_SKIP_DBS`, and restart.
  The database will resume processing from where it left off.
- **Permanently remove the database** by running `DETACH DATABASE <name>` while the database is dormant.
  This removes the database configuration from future blocks.

## SQL parser cache resets

XTDB's SQL parser caches what it learns about the grammar as it parses, so that later statements of a similar structure parse faster.
The cache is keyed on the structure of each statement — its sequence of keywords, operators and nesting — not on its values, so it stays small for applications that send the same few statement shapes.
SQL whose structure keeps varying grows it: most often, literal nested documents in `INSERT … RECORDS` or `VALUES`, whose structure follows the data.

The cache is shared by every node in the JVM, and is reset to empty once it holds more than a fixed number of states.
Each reset is logged:

```
SQL parser cache reset at 50,214 states (est. ~490MB) after 1,204,331 parses; see https://docs.xtdb.com/ops/troubleshooting.html#sql-parser-cache-resets
```

Resetting the cache costs speed, never correctness: statements parse slower until it re-warms.

`XTDB_SQL_PARSER_CACHE_MAX_STATES` (or the `xtdb.sql.parser-cache.max-states` system property)

: The number of states the cache holds before it is reset; default `50000`.
  `-1` never resets it.
  The environment variable takes precedence over the system property; a value that isn't a whole number is logged as a warning and the default used.

  Each state takes roughly 10KB of heap, so the default is roughly 500MB.
  This is an estimate: it varies with the SQL, and memory can briefly reach twice the limit while statements already parsing finish on the previous cache.

### Reading the resets

Occasional resets are the limit working as intended, and need no action.

Frequent resets — few parses between them — mean the workload's statement shapes outgrow the limit:

- If your statements embed literal values whose structure varies, such as nested documents, pass them as query parameters instead.
  The statement text then has one shape however the values vary, and the cache stops growing.
- If the shapes are stable but numerous, and the heap has room, raise `XTDB_SQL_PARSER_CACHE_MAX_STATES`.

The cache's size and reset count are exposed as the `xtdb.sql.parser.cache.states` gauge and the `xtdb.sql.parser.cache.resets` counter.
Both describe the JVM's one cache, so every node in the same JVM reports the same values.
