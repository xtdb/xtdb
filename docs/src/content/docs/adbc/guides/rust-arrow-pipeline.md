---
title: A Rust ADBC + arrow-rs pipeline against XTDB
description: A Rust ADBC pipeline where Arrow types are checked at compile time end to end.
---

Where Python ADBC preserves your DataFrame, Rust ADBC preserves your Rust types.
Query results land as `arrow::record_batch::RecordBatch` values: the same types DataFusion uses internally and the same ones `parquet::arrow::ArrowWriter` consumes.
There is no row-level decode and no schema duck-typing between XTDB and the rest of your pipeline.

This guide walks the full path:

1. Connect to XTDB's FlightSQL listener from Rust with `adbc_core` + `adbc_driver_manager`.
2. Stream a query result back as `RecordBatch`es.
3. Register those batches with DataFusion and run analytical SQL.
4. Write the aggregate out to Parquet via `parquet::arrow::ArrowWriter`.

The runnable crate is in [`examples/rust-arrow-pipeline/`](https://github.com/xtdb/xtdb/tree/main/docs/src/content/docs/adbc/examples/rust-arrow-pipeline).
It's a single file: point it at your node and change the SQL.

## Cargo.toml: a version matrix that resolves

The Rust ADBC ecosystem is younger than Python's, and the arrow / datafusion / parquet trio shares a single major version that all three crates must agree on.
Mix two arrow majors in one binary and `RecordBatch` types stop unifying, producing compile errors that point at trait-impl noise rather than the underlying version skew.

A pinned set that resolves:

```toml
[dependencies]
adbc_core = "=0.23.0"
adbc_driver_manager = "=0.23.0"
adbc-driver-flightsql = "=0.1.2"
arrow = "=58.3.0"
arrow-array = "=58.3.0"
arrow-schema = "=58.3.0"
parquet = "=58.3.0"
datafusion = "=53.1.0"
tokio = { version = "=1.52.3", features = ["rt-multi-thread", "macros"] }
```

- `adbc_core` / `adbc_driver_manager` 0.23 declare `arrow-array >=53.1, <59`, so they happily resolve against the arrow 58 that datafusion 53 pulls in.
  You don't need to override the resolver; do not pin arrow to 53 just because adbc_core's minimum is 53, since datafusion would still pull 58 alongside and you'd be back to two arrow majors in one binary.
- `adbc-driver-flightsql` is a *shim*: its `build.rs` downloads the official Apache `libadbc_driver_flightsql.{so,dylib,dll}` from the matching PyPI wheel and exposes its on-disk path as `DRIVER_PATH`.
  The crate defaults to native-driver version `1.9.0`, which is too old: the `adbc.ingest.target_table` / `adbc.ingest.mode` option keys landed in `1.10.0`, and several other rough edges (option dispatch, error formatting) were smoothed in `1.11.0`.
  Set `ADBC_FLIGHTSQL_VERSION=1.11.0` in the environment for the first `cargo build` and the wheel is cached for subsequent runs.

These versions are coupled, so the pins are exact (`=`): change them together as a set, not one at a time.

## Connecting

```rust
use adbc_core::{
    options::{AdbcVersion, OptionDatabase},
    Connection, Database, Driver, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::ManagedDriver;

let mut driver = ManagedDriver::load_dynamic_from_filename(
    DRIVER_PATH,
    None,
    AdbcVersion::default(),
)?;
let database = driver
    .new_database_with_opts([(OptionDatabase::Uri, "grpc://localhost:9832".into())])?;
let mut conn = database.new_connection()?;
```

`load_dynamic_from_filename` does the `dlopen` and resolves the entry point.
`ManagedDriver` implements `adbc_core::Driver` directly: every method below is the same trait you'd call against any other ADBC backend.

`AdbcVersion::default()` is `V110`, the version XTDB and the current native FlightSQL driver use.
If you ever see "Unknown statement option" errors that look intercepted client-side, the version is the first thing to check.

## Query → RecordBatch stream

`Statement::execute` returns `Box<dyn RecordBatchReader + Send + 'static>`, a streaming Arrow reader.
Iterate it batch-at-a-time for large results, or `collect` for small ones:

```rust
let mut q = conn.new_statement()?;
q.set_sql_query("SELECT _id, region, amount FROM orders")?;
let reader = q.execute()?;
let result_schema = reader.schema();
let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
```

The Flight gRPC framing copies the Arrow IPC bytes once at the gRPC boundary on each side.
After that the buffers are native Arrow, and DataFusion and parquet-rs operate on them directly without decoding or widening.
So this isn't zero-copy across the network boundary, but the data stays Arrow-shaped at every step within the process.

## Handing off to DataFusion

`SessionContext::register_batch` takes a `(name, RecordBatch)` pair and registers it as an in-memory table.
For multi-batch results, concatenate first with `arrow::compute::concat_batches` (one Vec scan, no copies of the column buffers, since it stitches the existing arrays into the new batch):

```rust
use datafusion::prelude::SessionContext;

let ctx = SessionContext::new();
ctx.register_batch(
    "orders",
    arrow::compute::concat_batches(&result_schema, &batches)?,
)?;

let df = ctx
    .sql("SELECT region, COUNT(*) AS n_orders, SUM(amount) AS total_amount
          FROM orders GROUP BY region ORDER BY total_amount DESC")
    .await?;
let agg_batches: Vec<RecordBatch> = df.collect().await?;
```

`df.collect().await?` runs the DataFusion plan and returns another `Vec<RecordBatch>`: same Arrow types, same buffer layout.
The Tokio runtime is here because DataFusion's surface is async; everything else in this pipeline is sync.

For multi-table queries (join an XTDB table against a Parquet file on disk, say) register each side independently and write the join in DataFusion SQL.
That's the main reason to put DataFusion in the middle rather than wiring the FlightSQL stream straight to a Parquet writer: it gives you a query layer over heterogeneous Arrow sources without leaving the Arrow type system.

## Writing Parquet

`parquet::arrow::ArrowWriter` takes the Arrow schema and accepts each `RecordBatch`:

```rust
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

let agg_schema = agg_batches.first().map(|b| b.schema())
    .ok_or("DataFusion returned no batches")?;
let file = std::fs::File::create("orders-by-region.parquet")?;
let mut writer = ArrowWriter::try_new(file, agg_schema, Some(WriterProperties::builder().build()))?;
for batch in &agg_batches {
    writer.write(batch)?;
}
writer.close()?;
```

`writer.close()` flushes the row-group footers: call it explicitly rather than relying on `Drop`.
Drop will swallow any error; `close()` returns one.

Properties default to snappy compression and reasonable row-group sizing; reach for `WriterProperties::builder()` knobs (codec, page size, dictionary encoding) when you have measurements that say you should.

## Lifecycle notes

A few Rust-specific corners worth knowing about:

- `ManagedDriver`, `ManagedDatabase`, `ManagedConnection`, `ManagedStatement` are all `Arc`-wrapped internally: cheap to clone, safe across threads.
  Don't reach for `Arc<Mutex<…>>` around them yourself.
- The `RecordBatchReader` returned by `execute()` borrows nothing from the statement; the statement can be dropped once the reader is constructed.
  Lifetime is `'static`, which is what makes `collect` and async handoff to DataFusion straightforward.
- `execute_update()` returns `Result<Option<i64>>`, where `None` means "row count unknown" rather than "zero rows".
  XTDB returns `None` for most DML; treat absence as success, not failure.
- The native FlightSQL driver library is loaded on first use and stays loaded for the process lifetime.
  Open one `ManagedDriver` at startup and reuse it; repeated `load_dynamic_from_filename` calls work but waste work.

## Running it

This needs the FlightSQL listener, which the `nightly` image enables by default on port 9832 (stable images from 2.2.0 onwards do too):

```sh
docker run --rm -d --name xtdb-g6 \
    -p 5432:5432 -p 9832:9832 \
    ghcr.io/xtdb/xtdb:nightly

cd docs/src/content/docs/adbc/examples/rust-arrow-pipeline
ADBC_FLIGHTSQL_VERSION=1.11.0 cargo run --release
```

Output:

```
seeded orders
got 1 batch(es), 6 row(s) total from XTDB

-- DataFusion result --
+--------+----------+--------------+
| region | n_orders | total_amount |
+--------+----------+--------------+
| us     | 3        | 540          |
| eu     | 2        | 350          |
| apac   | 1        | 175          |
+--------+----------+--------------+

wrote orders-by-region.parquet
```

The example seeds via plain SQL `INSERT` to stay self-contained.
To bulk-ingest an Arrow batch instead, set `adbc.ingest.target_table` and call `execute_update`.
This issues the same FlightSQL `CommandStatementIngest` the [bulk-ingest-from-parquet guide](./bulk-ingest-from-parquet) uses from Python, and works identically from any ADBC client.

## One type system end to end

Everything from `q.execute()?` through `writer.close()?` is the same Arrow type system.
`RecordBatch`, `Schema`, `Field`, `ArrayRef`: one set of types, one set of compile-time guarantees.
Add a `Vec<i64>` column to your query and DataFusion's SQL planner sees an `Int64` field, the parquet writer encodes an INT64 page, your downstream consumers see whatever they consume from arrow-rs.
No string-typed schema metadata, no per-column conversion adapters, no type-widening surprises.

Where the Python path preserves your DataFrame across the round trip, the Rust path preserves what `cargo check` verifies: the Arrow types line up at compile time.
