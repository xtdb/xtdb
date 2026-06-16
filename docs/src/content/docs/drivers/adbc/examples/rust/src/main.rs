//! Minimal XTDB-via-ADBC hello world in Rust.
//!
//! There is no pure-Rust FlightSQL driver: `adbc_driver_manager` dlopens the
//! *native* libadbc_driver_flightsql, and the `adbc-driver-flightsql` shim
//! crate downloads that library from the official PyPI wheel and exposes its
//! on-disk path as `DRIVER_PATH`.
//!
//! Requires a running XTDB node with the FlightSQL listener on localhost:9832.
//! The standalone Docker image enables this by default:
//!
//!     docker run --rm -p 5432:5432 -p 9832:9832 ghcr.io/xtdb/xtdb:nightly
//!
//! Run with:
//!     ADBC_FLIGHTSQL_VERSION=1.11.0 cargo run
//!
//! The env var only affects the first build: it tells the
//! `adbc-driver-flightsql` build script which native driver wheel to download.
//! Subsequent builds reuse the cached library.

use std::sync::Arc;

use adbc_core::{
    options::{AdbcVersion, IngestMode, OptionDatabase, OptionStatement},
    Connection, Database, Driver, Optionable, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::ManagedDriver;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

const FLIGHT_URI: &str = "grpc://localhost:9832";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the native FlightSQL driver and open an ADBC connection.
    let mut driver =
        ManagedDriver::load_dynamic_from_filename(DRIVER_PATH, None, AdbcVersion::default())?;
    let database =
        driver.new_database_with_opts([(OptionDatabase::Uri, FLIGHT_URI.into())])?;
    let mut conn = database.new_connection()?;

    // 1. Bulk-ingest an Arrow RecordBatch: one round trip, no row shredding.
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
            Arc::new(Int32Array::from(vec![30, 25, 28])),
        ],
    )?;
    let mut ingest = conn.new_statement()?;
    ingest.set_option(OptionStatement::TargetTable, "users".into())?;
    ingest.set_option(OptionStatement::IngestMode, IngestMode::CreateAppend.into())?;
    ingest.bind(batch)?;
    ingest.execute_update()?;
    println!("ingested users");

    // 2. Query the rows back as a stream of Arrow batches.
    let mut q = conn.new_statement()?;
    q.set_sql_query("SELECT _id, name, age FROM users ORDER BY _id")?;
    let reader = q.execute()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "got {} batch(es), {total_rows} row(s) total from XTDB",
        batches.len(),
    );

    println!("\n-- users --");
    arrow::util::pretty::print_batches(&batches)?;

    Ok(())
}
