//! XTDB → ADBC (FlightSQL) → DataFusion → Parquet, end-to-end, in Rust.
//!
//! Prereq: a running XTDB node with the FlightSQL listener reachable at
//!     grpc://localhost:9832
//! e.g.
//!     docker run --rm -d --name xtdb-g6 \
//!         -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
//!
//! Run with:
//!     ADBC_FLIGHTSQL_VERSION=1.11.0 cargo run --release
//!
//! (the env var only affects the first build: it tells the
//! `adbc-driver-flightsql` build script which native driver wheel to
//! download. Subsequent builds reuse the cached library.)

use adbc_core::{
    options::{AdbcVersion, OptionDatabase},
    Connection, Database, Driver, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::ManagedDriver;
use arrow_array::RecordBatch;
use datafusion::prelude::SessionContext;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

const FLIGHT_URI: &str = "grpc://localhost:9832";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---------------------------------------------------------------
    // 1. Load the native FlightSQL driver and open an ADBC connection.
    //    `adbc-driver-flightsql` ships the conda-forge native library and
    //    exposes its on-disk path; `adbc_driver_manager` dlopens it.
    // ---------------------------------------------------------------
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        DRIVER_PATH,
        None,
        AdbcVersion::default(),
    )?;
    let database = driver
        .new_database_with_opts([(OptionDatabase::Uri, FLIGHT_URI.into())])?;
    let mut conn = database.new_connection()?;

    // ---------------------------------------------------------------
    // 2. Seed a table to read back. Plain SQL keeps this script
    //    self-contained; bulk-ingesting an Arrow batch via
    //    `adbc.ingest.target_table` is covered in the
    //    `bulk-ingest-from-parquet` guide.
    // ---------------------------------------------------------------
    let mut seed = conn.new_statement()?;
    seed.set_sql_query(
        "INSERT INTO orders (_id, region, amount) VALUES
           ('o-1', 'eu',   100),
           ('o-2', 'eu',   250),
           ('o-3', 'us',    80),
           ('o-4', 'us',   400),
           ('o-5', 'us',    60),
           ('o-6', 'apac', 175)",
    )?;
    seed.execute_update()?;
    println!("seeded orders");

    // ---------------------------------------------------------------
    // 3. Query XTDB back as a stream of Arrow batches.
    //    `execute` returns a Box<dyn RecordBatchReader>; collect into a Vec
    //    so DataFusion can register them as an in-memory table.
    //    The Arrow IPC framing on the FlightSQL wire does one copy at the
    //    gRPC boundary. Past that, every downstream step (DataFusion,
    //    parquet-rs) operates on the same Arrow buffers without re-decode.
    // ---------------------------------------------------------------
    let mut q = conn.new_statement()?;
    q.set_sql_query("SELECT _id, region, amount FROM orders")?;
    let reader = q.execute()?;
    let result_schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    println!(
        "got {} batch(es), {} row(s) total from XTDB",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
    );

    // ---------------------------------------------------------------
    // 4. Hand the batches to DataFusion and run analytical SQL on them.
    //    The schema and the batches are still Arrow: the same Rust
    //    types DataFusion uses internally, no conversion.
    // ---------------------------------------------------------------
    let ctx = SessionContext::new();
    ctx.register_batch(
        "orders",
        arrow::compute::concat_batches(&result_schema, &batches)?,
    )?;

    let df = ctx
        .sql(
            "SELECT region,
                    COUNT(*)    AS n_orders,
                    SUM(amount) AS total_amount
             FROM orders
             GROUP BY region
             ORDER BY total_amount DESC",
        )
        .await?;
    let agg_batches: Vec<RecordBatch> = df.collect().await?;
    println!("\n-- DataFusion result --");
    arrow::util::pretty::print_batches(&agg_batches)?;

    // ---------------------------------------------------------------
    // 5. Persist the aggregate to Parquet via parquet-rs.
    //    Same arrow-rs schema and batches feed straight in.
    // ---------------------------------------------------------------
    let agg_schema = agg_batches
        .first()
        .map(|b| b.schema())
        .ok_or("DataFusion returned no batches")?;
    let out_path = "orders-by-region.parquet";
    let file = std::fs::File::create(out_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, agg_schema, Some(props))?;
    for batch in &agg_batches {
        writer.write(batch)?;
    }
    writer.close()?;
    println!("\nwrote {out_path}");

    Ok(())
}
