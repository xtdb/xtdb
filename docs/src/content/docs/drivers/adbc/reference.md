---
title: ADBC reference
description: Reference for connecting to XTDB via ADBC, in-process and over FlightSQL.
---

XTDB exposes [ADBC](https://arrow.apache.org/adbc/), the Arrow Database Connectivity standard, through two surfaces:

- **In-process**, via `xtdb.adbc.XtdbConnection` on the JVM. Zero copies, direct access to the in-memory Arrow buffers.
- **Over the wire**, via the [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) server bundled into XTDB.
  Any ADBC FlightSQL driver (Python, Rust, Go, C, R, Java) connects to it.

The two paths share the same code below the ADBC surface: the FlightSQL producer is a thin shim that delegates to the same `XtdbStatement` lifecycle the in-process client uses.
If something works in-process, it works over the wire, and vice versa, modulo the per-client caveats called out below.

## When to reach for ADBC

XTDB also exposes a PostgreSQL wire-compatible server.
The [pgwire path](/drivers) fits if your tooling is already Postgres-compatible.
Reach for ADBC when:

- **You want Arrow through the whole pipeline.**
  Query results land in your client as Arrow batches, with no row-by-row decode and no type widening through a PG text wire format.
- **You want to bulk-ingest Arrow data.**
  `cur.adbc_ingest(arrow_table, mode='create_append')` lands a `pyarrow.Table` (or any Arrow-shaped data) in XTDB in a single round trip, without shredding it into per-row `executemany("INSERT ... VALUES (?, ...)")` calls.
- **You want zero-copy result shipping** between XTDB, pandas / polars / DuckDB / DataFusion / your own Arrow-shaped compute.
  The data stays in Arrow across each hop.

For interactive SQL, `psql`-style tooling, or anything Postgres-ecosystem-shaped, the [pgwire driver](/drivers) is still the right answer.

## Connecting

### In-process (Kotlin / JVM)

`XtdbConnection` is the entry point.
Obtain one from a running `Xtdb` node:

```kotlin
import xtdb.adbc.XtdbConnection
import xtdb.api.Xtdb

val node = Xtdb.openNode()
val conn: XtdbConnection = node.openAdbcConnection()

conn.use {
    val stmt = it.createStatement()
    stmt.setSqlQuery("SELECT 1")
    val result = stmt.executeQuery()
    // result.reader is an org.apache.arrow.vector.ipc.ArrowReader
}
```

`XtdbConnection` implements `org.apache.arrow.adbc.core.AdbcConnection`: anything written against the ADBC Java API works against it directly.

### Over the wire (FlightSQL)

Enable the FlightSQL listener in your node config; the [Docker standalone image](/intro/installation-via-docker) does this by default on port `9832`:

```yaml
flightSql:
  host: '*'
  port: 9832
```

Then connect with any ADBC FlightSQL driver.

**Python** ([`adbc_driver_flightsql`](https://pypi.org/project/adbc-driver-flightsql/)):

```python
import adbc_driver_flightsql.dbapi as flight_sql

with flight_sql.connect("grpc://localhost:9832") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchall())
```

**Rust** (`adbc_core` + `adbc_driver_manager`):

There is no pure-Rust FlightSQL driver: `adbc_driver_manager` dlopens the *native* `libadbc_driver_flightsql`.
The [`adbc-driver-flightsql`](https://crates.io/crates/adbc-driver-flightsql) shim crate downloads that library from the official PyPI wheel and exposes its on-disk path as `DRIVER_PATH`.

```rust
use adbc_core::{
    options::{AdbcVersion, OptionDatabase},
    Connection, Database, Driver, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::ManagedDriver;

let mut driver =
    ManagedDriver::load_dynamic_from_filename(DRIVER_PATH, None, AdbcVersion::default())?;
let database = driver.new_database_with_opts([
    (OptionDatabase::Uri, "grpc://localhost:9832".into()),
])?;
let conn = database.new_connection()?;
let mut stmt = conn.new_statement()?;
stmt.set_sql_query("SELECT 1")?;
let reader = stmt.execute()?;
```

The shim's first build downloads a ~5 MB native library; set `ADBC_FLIGHTSQL_VERSION=1.11.0` for that build.
See the [rust-arrow-pipeline example](/drivers/adbc/guides/rust-arrow-pipeline) for the full dependency matrix and an end-to-end ingest/query pipeline.

**Go** (`github.com/apache/arrow-adbc/go/adbc/driver/flightsql`):

```go
import (
    "github.com/apache/arrow-adbc/go/adbc"
    "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
)

drv := flightsql.NewDriver(memory.DefaultAllocator)
db, _ := drv.NewDatabase(map[string]string{
    adbc.OptionKeyURI: "grpc://localhost:9832",
})
conn, _ := db.Open(ctx)
defer conn.Close()
```

Same address works for the C and R drivers; see [the Apache ADBC driver matrix](https://arrow.apache.org/adbc/current/driver/flight_sql.html) for client-specific install instructions.

> **TLS / auth.** This reference covers the plaintext gRPC path.
> Production deployments should front the FlightSQL listener with TLS termination.
> XTDB's authentication (see [Authentication](/ops/config/authentication)) applies to the pgwire listener, not currently to FlightSQL.

## Supported surfaces

### Statements: prepare / bind / execute

`AdbcStatement` is the entry point for every query and DML.
The lifecycle is the standard ADBC one:

```python
cur = conn.cursor()
cur.execute_partitions("SELECT ?, ?", parameters=[42, "hello"])
```

or, in raw ADBC terms:

```
stmt.setSqlQuery(sql)
stmt.prepare()                    # optional, but required if you're binding params
stmt.bind(arrow_record_batch)     # binds one batch of parameters
stmt.executeQuery()  →  ArrowReader
```

Query results stream: `executeQuery()` returns an `ArrowReader` that yields one Arrow batch at a time, so large result sets don't have to fit in memory.

`executeUpdate()` runs DML; the return value is `-1` (XTDB does not pre-count) and effects are visible to the next query on the same connection (see [Transactions](#transactions) for multi-statement semantics).

### `executeSchema()`: result shape without executing

`AdbcStatement.executeSchema()` returns the Arrow schema of a query's result set *without* running the query.
Useful for tooling that wants to introspect a query's shape: IDE autocomplete, dataframe pipeline builders, anything that wants to know "what columns would come back".

```python
cur.execute("SELECT _id, name FROM users WHERE _id = ?", parameters=["alice"])
schema = cur.adbc_get_result_schema()
# Schema { _id: utf8, name: utf8 }
```

Works both on prepared statements (the ADBC client reads it from `getResultSetSchema` on the prepared handle) and ad-hoc (the wire path opens a transient `PreparedQuery` to read the shape).

**Caveat: parameter types.**
Result schemas can depend on parameter types (`SELECT ?` is the obvious case).
`executeSchema()` runs before any bind, so we feed the planner null-typed placeholder params.
For `SELECT cols FROM t WHERE _id = ?` (the common shape, where the projection doesn't depend on the parameter) this is accurate.
For `SELECT ?` / `SELECT ? + 1` (projecting the parameter directly) you'll get placeholder types: `null` rather than the type you'd bind.
Workaround: project through a column expression that fixes the type.

### `getObjects` / `getTables` / `getTableSchema`

Metadata discovery is fully wired.
All three of the standard ADBC metadata calls work:

```python
# List schemas
reader = conn.adbc_get_objects(depth="db_schemas")

# List tables in a schema
reader = conn.adbc_get_objects(depth="tables", db_schema_filter="public")

# Full structure including columns
reader = conn.adbc_get_objects(depth="all", table_name_filter="users")

# Just the schema of one table
schema = conn.adbc_get_table_schema("users", db_schema_filter="public")
```

The full-tree (`depth=all`) response carries each table's Arrow Schema as bytes in the standard `table_schema` `VarBinaryVector`: your client deserialises it into an actual `pyarrow.Schema` / `arrow::Schema`.

Column types reflect **live data**, not just flushed-to-block state: freshly-inserted rows show up in `getObjects` / `getTableSchema` immediately, the same way they show up via SQL `SELECT`.

### Bulk ingest

Land a `pyarrow.Table` (or any Arrow-shaped data: record batches, streams, any object exposing the Arrow C Data Interface) in XTDB in a single round trip:

```python
import pyarrow as pa

people = pa.table({
    "_id": ["alice", "bob"],
    "name": ["Alice", "Bob"],
    "age":  [30, 25],
})

with conn.cursor() as cur:
    cur.adbc_ingest("people", people, mode="create_append")
```

Arrow batches stream from the client into XTDB one batch at a time, the whole table doesn't need to fit in memory either side.

**Mode handling.**
Because XTDB creates tables on demand, several of ADBC's ingest modes either coincide or don't apply:

| Mode | Behaviour |
|------|-----------|
| `create` | Accepted. Table is auto-created if missing. |
| `append` | Accepted. Upserts on `_id`. |
| `create_append` | Accepted (the common case). |
| `replace` | **Rejected** (`INVALID_ARGUMENT`). Would require an explicit `ERASE` step. |
| `create` with fail-if-exists | **Rejected.** XTDB auto-creates; we can't honour "fail if exists" without an existence check. |
| `append` with fail-if-not-exists | **Rejected.** XTDB auto-creates on insert; silently accepting would violate the ADBC contract. |

Mode rejections (the rows above) come back as gRPC `INVALID_ARGUMENT` with a descriptive message.

Other ingest-time failures are not yet uniformly mapped: a row missing its `_id` column currently surfaces as gRPC `INTERNAL` rather than `INVALID_ARGUMENT` (see below).
Match on the message, not the status code, until that mapping is tightened.

**Other narrowings.**

- Per-call **catalog override** is rejected: the connection-scoped catalog (via session options or the connection URI) is the only source of truth.
  Per-call **schema override** is honoured; defaults to `public`.
- Every row must have an `_id` column.
  XTDB's data model requires it; rows without `_id` are rejected (currently as gRPC `INTERNAL`; see the note above).
  If you're round-tripping Arrow tables that don't have one, materialise an `_id` column client-side before calling `adbc_ingest`.

### `current_catalog` / `current_db_schema`

Standard ADBC session options, exposed over the wire:

```python
conn.adbc_current_catalog       # 'xtdb'
conn.adbc_current_db_schema     # 'public'
```

Both work from any Go-driver-based ADBC client (Python, C, R, Go itself).

**Java caveat.**
The Apache ADBC Java client (`adbc-driver-flight-sql` 0.23 and earlier) doesn't query `getSessionOptions`; it inherits `AdbcConnection`'s default which raises `notImplemented`.
This is an upstream gap, not an XTDB one: Java's `getCurrentCatalog` / `getCurrentDbSchema` on the wire client will stay unsupported until Apache ADBC wires it up.
The **in-process** Kotlin/JVM `XtdbConnection` is unaffected: `getCurrentCatalog()` / `getCurrentDbSchema()` work directly.

**Setting** the catalog, via `setSessionOptions("catalog", db)`, selects the database for the session.
This is how Go-driver-based clients (Python, C, R, Go) pick a database: the catalog session option selects it for the session.
The value is validated against the node's known databases: an unknown name comes back `INVALID_VALUE`, an empty string clears it.

Setting it creates a FlightSQL session and the server issues a session cookie; the client must keep that cookie across calls for the option to persist.
`getSessionOptions` is a pure getter: it reports the resolved catalog and `public` schema, and deliberately does *not* create a session, so probing for the current catalog doesn't leak one.

The **schema** is not settable: `public` is the only accepted value (a confirming no-op), anything else is rejected.

`closeSession` ends the session: it closes the session's connections (autocommit and any open transaction) and invalidates the cookie.

### Transactions

Multi-statement transactions work both in-process and over the wire.

Over the wire, switch off autocommit on the underlying ADBC connection, run your statements, then commit (or roll back):

```python
conn.adbc_connection.set_autocommit(False)        # BeginTransaction
with conn.cursor() as cur:
    cur.executemany(
        "INSERT INTO users (_id, name) VALUES (?, ?)",
        [["alice", "Alice"], ["bob", "Bob"]],
    )
conn.adbc_connection.commit()                      # EndTransaction COMMIT
```

DML goes through `executemany`, not `execute`: the Python dbapi routes `execute()`, including `INSERT` / `UPDATE` / `DELETE`, through the FlightSQL query path, which XTDB rejects; `executemany` routes through the update path (`DoPut`).
In-process and in Kotlin / Rust / Go, the same flow is `setAutoCommit(false)` / `commit()` / `rollback()` on the ADBC connection.
See [Prepared statements and transactions](/drivers/adbc/guides/prepared-statements-and-transactions) for the full lifecycle, rollback semantics, and same-connection visibility.

**Default commit mode.**
Because XTDB advertises FlightSQL transaction support (`FLIGHT_SQL_SERVER_TRANSACTION`, SqlInfo id 8), the Python dbapi follows PEP 249 and defaults to **manual-commit**.
DML run via `execute` / `executemany` stays in an uncommitted transaction until you call `conn.commit()`.
For ingest-and-query scripts that aren't managing transactions, pass `autocommit=True` to `flight_sql.connect(...)`.
`adbc_ingest` is the exception: each bulk-ingest call commits atomically on its own, regardless of the connection's commit mode.

**Visibility inside an open transaction.**
XTDB buffers a transaction's DML and applies it atomically on `COMMIT`, so reads on the same connection do **not** see that transaction's own pending writes before it commits.
A `SELECT` issued between an uncommitted `INSERT` and the `COMMIT` returns the pre-transaction state.

**Same-connection write-then-read (autocommit / committed writes).**
Once a write is committed (in autocommit mode, or after `commit()`), subsequent reads on the same connection see it without a manual `await`.
The connection tracks its own write tokens and threads them through the planner so a committed `INSERT` followed by `SELECT` (or by `executeSchema`, or by `getTableSchema`) sees the just-written rows.
Cross-connection visibility follows the usual transactional semantics: you'll see another connection's writes once they commit and your read snapshot advances.

### `getTableTypes` / `getInfo`

Standard ADBC metadata calls:

- `getTableTypes()` returns `TABLE` (XTDB has one table type).
- `getInfo(...)` returns `VENDOR_NAME = "XTDB"`, `DRIVER_NAME = "XTDB ADBC Driver"`, and version fields.
  Version fields are placeholder strings.

## What isn't supported

- **Setting the `schema` session option**: `public` is the only accepted value; see the session-options section above. (Setting the `catalog` and `closeSession` *are* supported.)
- **Bulk ingest `replace` mode and existence-check modes**: rejected with explanatory `INVALID_ARGUMENT`; see above.
- **Per-call catalog override on bulk ingest**: rejected; use the connection-scoped catalog.
- **Substrait variant of `executeSchema`** (`getSchemaSubstraitPlan`): no current driver consumer.
- **Java FlightSQL client `getCurrentCatalog` / `getCurrentDbSchema`**: upstream Apache ADBC gap, not XTDB.
- **Parameter type inference for `executeSchema`**: placeholder types only; see the caveat under `executeSchema()`.
- **TLS / auth on the FlightSQL listener**: not currently supported; front it with a TLS-terminating proxy (see the deployment note above).

## One ADBC implementation with two surfaces

```d2 sketch="false" theme="200" title="ADBC: two surfaces over one implementation"
direction: down

client: "Your client\nPython / Rust / Go / C / R / Java …"
producer: "XtdbProducer\nFlightSQL → ADBC shim"
impl: "XtdbConnection / XtdbStatement\nimplements org.apache.arrow.adbc.core.*"
node: XTDB node

client -> producer: "FlightSQL (gRPC)"
producer -> impl: "delegates"
impl -> node
client -> impl: "in-process (JVM)" {
  style.stroke-dash: 3
}
```

The wire path (FlightSQL) and the in-process JVM path resolve to the same `XtdbConnection` / `XtdbStatement` implementation.
Anything you can do via the in-process `AdbcConnection` you can do over the wire, and vice versa, barring the wire-client caveats above.

## Examples

Minimal runnable examples are in [`docs/drivers/adbc/examples/`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/drivers/adbc/examples): one `hello-world` per language.
For more end-to-end material see:

- [Tutorial](./tutorial): end-to-end notebook-style walkthrough.
- [How-to guides](./guides): task-shaped recipes (Parquet bulk ingest, pandas/polars round-trip, etc.).
- [`xtdb/driver-examples`](https://github.com/xtdb/driver-examples): the canonical multi-language driver-validation suite.
