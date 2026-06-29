---
title: ADBC reference
description: The ADBC surface XTDB supports, in-process and over FlightSQL, with its XTDB-specific behaviour.
---

XTDB exposes [ADBC](https://arrow.apache.org/adbc/), the Arrow Database Connectivity standard, through two surfaces:

- **In-process**, via `node.connect()` on the JVM, which returns an `org.apache.arrow.adbc.core.AdbcConnection`. Zero copies, direct access to the in-memory Arrow buffers.
- **Over the wire**, via the [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server bundled into XTDB. Any ADBC FlightSQL driver (Python, Rust, Go, C, R, Java) connects to it.

The FlightSQL producer is a thin shim over the same in-process `AdbcConnection` / `AdbcStatement` implementation, so a behaviour documented here holds on either surface, barring the per-client caveats noted below.
Signatures and examples are given in the in-process Kotlin API; over the wire the same operations are whatever your driver's ADBC binding calls them.

## Connecting

### In-process (Kotlin / JVM)

An `org.apache.arrow.adbc.core.AdbcConnection` is obtained from a running node via `node.connect()`:

```kotlin
import xtdb.api.Xtdb

Xtdb.openNode().use { node ->
    node.connect().use { conn ->
        conn.createStatement().use { stmt ->
            stmt.setSqlQuery("SELECT 1")
            stmt.executeQuery().use { result ->
                // result.reader: org.apache.arrow.vector.ipc.ArrowReader
            }
        }
    }
}
```

### Over the wire (FlightSQL)

Enable the FlightSQL listener in your node config; the [Docker standalone image](/intro/installation-via-docker) does this by default on port `9832`:

```yaml
flightSql:
  host: '*'
  port: 9832
```

Then connect with any ADBC FlightSQL driver pointed at `grpc://localhost:9832`.
Connection setup is driver-specific: see the connection snippet on the [Python](/drivers/python#arrow-native-access-via-adbc), [Java](/drivers/java#arrow-native-access-via-adbc), [Kotlin](/drivers/kotlin#arrow-native-access-via-adbc), or [Go](/drivers/go#arrow-native-access-via-adbc) driver page, or the [Apache ADBC driver matrix](https://arrow.apache.org/adbc/current/driver/flight_sql.html) for other languages.

TLS and authentication are not applied to the FlightSQL listener (XTDB's [authentication](/ops/config/authentication) covers the pgwire listener only); front it with a TLS-terminating proxy in production.

## Statements

`createStatement()`
: opens an `AdbcStatement`, the entry point for every query and DML.

`setSqlQuery(sql)` / `prepare()` / `bind(batch)` / `executeQuery()`
: the standard ADBC statement lifecycle. `prepare()` is optional but required before `bind()`; `bind()` takes one Arrow record batch of parameters. `executeQuery()` returns an `ArrowReader` that yields one Arrow batch at a time, so large result sets need not fit in memory.

```kotlin
val stmt = conn.createStatement()
stmt.setSqlQuery("SELECT ?, ?")
stmt.prepare()
stmt.bind(argBatch)
val result = stmt.executeQuery()
```

`executeUpdate()`
: runs DML. Returns `-1` (XTDB does not pre-count affected rows). Effects are visible to the next query on the same connection (see [Transactions](#transactions) for multi-statement semantics).

`executeSchema()`
: returns the Arrow schema of a query's result set without running it, for tooling that needs the result columns up front. Works on prepared statements (read from `getResultSetSchema`) and ad-hoc queries (the wire path opens a transient `PreparedQuery`).

    Because it runs before any bind, a result schema that depends on parameter types is resolved against null-typed placeholders. For `SELECT cols FROM t WHERE _id = ?` (where the projection doesn't depend on the parameter) the schema is accurate; a query that projects a parameter directly (`SELECT ?`, `SELECT ? + 1`) comes back `null`-typed for those fields. Project through a column expression that fixes the type if you need it resolved.

## Bulk ingest

`bulkIngest(table, mode)`
: lands an Arrow table (or any Arrow-shaped data: record batches, streams, anything exposing the Arrow C Data Interface) in `table` in a single round trip. Over the wire it maps to the FlightSQL `CommandStatementIngest` command; batches stream in one at a time, so the whole table needn't fit in memory at either end. Each call commits atomically on its own, regardless of the connection's commit mode.

```kotlin
conn.bulkIngest("people", BulkIngestMode.CREATE_APPEND).use { stmt ->
    stmt.bind(peopleBatch)        // _id, name, age …
    stmt.executeUpdate()
}
```

Because XTDB creates tables on demand, the ingest modes either coincide or don't apply:

| Mode | Behaviour |
|------|-----------|
| `create` | Accepted. Table is auto-created if missing. |
| `append` | Accepted. Upserts on `_id`. |
| `create_append` | Accepted (the common case). |
| `replace` | **Rejected** (`INVALID_ARGUMENT`). Would require an explicit `ERASE` step. |
| `create` with fail-if-exists | **Rejected.** XTDB auto-creates; "fail if exists" can't be honoured without an existence check. |
| `append` with fail-if-not-exists | **Rejected.** XTDB auto-creates on insert; silently accepting would violate the ADBC contract. |

Further constraints:

- Every row must have an `_id` column. Rows without one are rejected; materialise an `_id` client-side before ingesting if your source Arrow lacks one.
- A temporal `_valid_from` / `_valid_to` column must be a `timestamp`, not a `date`.
- A per-call catalog override is rejected (the connection-scoped catalog is the only source of truth); a per-call schema override is honoured, defaulting to `public`.

Mode rejections return a descriptive gRPC `INVALID_ARGUMENT`. Other ingest-time failures are not yet uniformly mapped: a missing `_id` surfaces as gRPC `INTERNAL`, so match on the message, not the status code, until that mapping is tightened.

## Transactions

`autoCommit = false` / `commit()` / `rollback()`
: multi-statement transactions, on both surfaces. Switch off autocommit, run statements, then commit or roll back on the connection.

```kotlin
conn.autoCommit = false
conn.createStatement().use { stmt ->
    stmt.setSqlQuery("INSERT INTO users (_id, name) VALUES ('alice', 'Alice')")
    stmt.executeUpdate()
}
conn.commit()
```

Because XTDB advertises FlightSQL transaction support (`FLIGHT_SQL_SERVER_TRANSACTION`, SqlInfo id 8), a PEP 249 wire client (e.g. the Python dbapi) defaults to manual-commit: DML stays in an uncommitted transaction until you commit. `bulkIngest` is the exception (each call commits on its own).

**Visibility inside an open transaction.**
XTDB buffers a transaction's DML and applies it atomically on `COMMIT`, so reads on the same connection do **not** see that transaction's own pending writes before it commits. A `SELECT` issued between an uncommitted `INSERT` and the `COMMIT` returns the pre-transaction state.

**Committed-write visibility.**
Once a write is committed (in autocommit mode, or after `commit()`), subsequent reads on the same connection see it with no manual `await`: the connection tracks its own write tokens and threads them through the planner. Cross-connection visibility follows the usual transactional semantics; another connection sees the write once it commits and its read snapshot advances.

See [Prepared statements and transactions](/adbc/guides/prepared-statements-and-transactions) for the full lifecycle, including the `execute` vs `executemany` DML routing on the Python dbapi.

## Metadata

`getObjects(depth, …)`
: returns the catalog → schema → table → column tree, to the requested `depth`. At `depth = ALL` each table's Arrow schema is carried as bytes in the standard `table_schema` `VarBinaryVector`, which the client deserialises into a `Schema`.

`getTableSchema(catalog, schema, table)`
: returns the Arrow schema of a single table.

```kotlin
val objs   = conn.getObjects(GetObjectsDepth.ALL, null, "public", "users", null, null)
val schema = conn.getTableSchema(null, "public", "users")
```

Both reflect **live data**, not just flushed-to-block state: freshly-inserted rows show up immediately, the same way they do via SQL `SELECT`.

`getTableTypes()`
: returns `TABLE` (XTDB has one table type).

`getInfo(…)`
: returns `VENDOR_NAME = "XTDB"`, `DRIVER_NAME = "XTDB ADBC Driver"`, and version fields (currently placeholder strings).

## Session options

`getCurrentCatalog()` / `getCurrentDbSchema()`
: return the session catalog (default `xtdb`) and schema (`public`). Read via `getSessionOptions`, a pure getter that does not create a session, so probing doesn't leak one.

`setSessionOptions("catalog", db)`
: selects the database for the session. The value is validated against the node's known databases: an unknown name returns `INVALID_VALUE`, an empty string clears it. Setting it creates a FlightSQL session; the server issues a cookie the client must keep across calls for the option to persist.

`closeSession()`
: ends the session, closes its connections, and invalidates the cookie.

The schema is not settable: `public` is the only accepted value (a confirming no-op), anything else is rejected.

## Not supported

- Setting the `schema` session option to anything but `public`.
- Bulk-ingest `replace` and existence-check modes (rejected with an explanatory `INVALID_ARGUMENT`).
- Per-call catalog override on bulk ingest.
- The Substrait variant of `executeSchema` (`getSchemaSubstraitPlan`): no current driver consumer.
- `getCurrentCatalog` / `getCurrentDbSchema` on the Java FlightSQL client: an upstream Apache ADBC gap (its Java client doesn't query `getSessionOptions`), not an XTDB limitation. The in-process JVM connection is unaffected.
- Parameter type inference for `executeSchema` (placeholder types only).
- TLS / auth on the FlightSQL listener.
